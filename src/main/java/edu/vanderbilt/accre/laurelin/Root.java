package edu.vanderbilt.accre.laurelin;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.vanderbilt.accre.laurelin.cache.Cache;
import edu.vanderbilt.accre.laurelin.cache.CacheFactory;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;
import edu.vanderbilt.accre.laurelin.root_proxy.IOFactory;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTException.UnsupportedBranchTypeException;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranchInterface;
import edu.vanderbilt.accre.laurelin.spark_ttree.StructColumnVector;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeColumnVector;

public class Root implements DataSourceV2, ReadSupport, DataSourceRegister {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Represents a Partition of a TTree.
     *
     * <p>This is instantiated on the driver, then serialized and transmitted to
     * the executor
     */
    static class TTreeDataSourceV2Partition implements InputPartition<ColumnarBatch> {
        private static final long serialVersionUID = -6598704946339913432L;
        private StructType schema;
        private long entryStart;
        private long entryEnd;
        private Map<String, SlimTBranch> slimBranches;
        private CacheFactory basketCacheFactory;
        private int threadCount;
        private CollectionAccumulator<Storage> profileData;
        private int pid;

        public TTreeDataSourceV2Partition(StructType schema, CacheFactory basketCacheFactory, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
            logger.trace("dsv2partition new");
            this.schema = schema;
            this.basketCacheFactory = basketCacheFactory;
            this.entryStart = entryStart;
            this.entryEnd = entryEnd;
            this.slimBranches = slimBranches;
            this.threadCount = threadCount;
            this.profileData = profileData;
            this.pid = pid;
        }

        @Override
        public InputPartitionReader<ColumnarBatch> createPartitionReader() {
            logger.trace("input partition reader");
            return new TTreeDataSourceV2PartitionReader(basketCacheFactory, schema, entryStart, entryEnd, slimBranches, threadCount, profileData, pid);
        }

        public void setPid(int pid) {
            this.pid = pid;
        }
    }

    static class TTreeDataSourceV2PartitionReader implements InputPartitionReader<ColumnarBatch> {
        private Cache basketCache;
        private StructType schema;
        private long entryStart;
        private long entryEnd;
        private int currBasket = -1;
        private Map<String, SlimTBranch> slimBranches;

        /**
         * ThreadPool handling the async decompression tasks
         */
        private static ThreadPoolExecutor staticExecutor;

        /**
         *  (very) surprisingly, a static thread pool executor will prevent the
         *  JVM from ever properly shutting down, because of a circular nature
         *  of the references. Static variables and thread objects are GC roots,
         *  and the thread pool object references its child threads while the
         *  threads themselves reference the threadpool. No amount of GC runs
         *  will make anything unreachable, so the JVM can't finalize and
         *  shut down. To break the cycle, we need to forcibly shutdown the
         *  thread pool, which causes it to kill its threads and allows the GC
         *  to unravel the references.
         *  <p>
         *  see also: https://stackoverflow.com/a/10395700
         */
        static {
            ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("laurelin-arraybuilder-%d").build();
            staticExecutor = new ThreadPoolExecutor(1, 1,
                                                    5L, TimeUnit.SECONDS,
                                                    new LinkedBlockingQueue<Runnable>(),
                                                    factory);
            staticExecutor.allowCoreThreadTimeOut(true);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    TTreeDataSourceV2PartitionReader.staticExecutor.shutdownNow();
                }
            });
        }

        /**
         * Holds the async threadpool if enabled, null otherwise
         */
        private static ThreadPoolExecutor executor;
        private CollectionAccumulator<Storage> profileData;
        private int pid;
        private static ROOTFileCache fileCache = new ROOTFileCache();

        public TTreeDataSourceV2PartitionReader(CacheFactory basketCacheFactory, StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
            this.basketCache = basketCacheFactory.getCache();
            this.schema = schema;
            this.entryStart = entryStart;
            this.entryEnd = entryEnd;
            this.slimBranches = slimBranches;
            this.profileData = profileData;
            this.pid = pid;

            Function<Event, Integer> cb = null;
            if (this.profileData != null) {
                cb = e -> {
                    this.profileData.add(e.getStorage());
                    return 0;
                };
            }
            IOProfile.getInstance(pid, cb);

            if (threadCount >= 1) {
                executor = staticExecutor;
                executor.setCorePoolSize(threadCount);
                executor.setMaximumPoolSize(threadCount);
            } else {
                executor = null;
            }
        }

        @Override
        public void close() throws IOException {
            logger.trace("close");
            // This will eventually go away due to GC, should I add
            // explicit closing too?
        }

        @Override
        public boolean next() throws IOException {
            logger.trace("next");
            if (currBasket == -1) {
                // nothing read yet
                currBasket = 0;
                return true;
            } else {
                // we already read the partition
                return false;
            }
        }

        @Override
        public ColumnarBatch get() {
            logger.trace("columnarbatch");
            LinkedList<ColumnVector> vecs = new LinkedList<ColumnVector>();
            vecs = getBatchRecursive(schema.fields());
            // This is miserable
            ColumnVector[] tmp = new ColumnVector[vecs.size()];
            int idx = 0;
            for (ColumnVector vec: vecs) {
                tmp[idx] = vec;
                idx += 1;
            }
            // End misery
            ColumnarBatch ret = new ColumnarBatch(tmp);
            ret.setNumRows((int) (entryEnd - entryStart));
            return ret;
        }

        private LinkedList<ColumnVector> getBatchRecursive(StructField[] structFields) {
            LinkedList<ColumnVector> vecs = new LinkedList<ColumnVector>();
            for (StructField field: structFields)  {
                if (field.dataType() instanceof StructType) {
                    LinkedList<ColumnVector> nestedVecs = getBatchRecursive(((StructType)field.dataType()).fields());
                    vecs.add(new StructColumnVector(field.dataType(), nestedVecs));
                    continue;
                }
                SlimTBranchInterface slimBranch = slimBranches.get(field.name());
                SimpleType rootType;
                rootType = SimpleType.fromString(field.metadata().getString("rootType"));

                Dtype dtype = SimpleType.dtypeFromString(field.metadata().getString("rootType"));
                vecs.add(new TTreeColumnVector(field.dataType(), rootType, dtype, basketCache, entryStart, entryEnd, slimBranch, executor, fileCache));
            }
            return vecs;
        }
    }

    public static class TTreeDataSourceV2Reader implements DataSourceReader,
            SupportsScanColumnarBatch,
            SupportsPushDownRequiredColumns {
        private LinkedList<String> paths;
        private String treeName;
        private TTree currTree;
        private TFile currFile;
        private CacheFactory basketCacheFactory;
        private StructType schema;
        private int threadCount;
        private IOProfile profiler;
        private static CollectionAccumulator<Storage> profileData;
        private SparkContext sparkContext;
        private static ROOTFileCache fileCache = new ROOTFileCache();

        public TTreeDataSourceV2Reader(DataSourceOptions options, CacheFactory basketCacheFactory, SparkContext sparkContext, CollectionAccumulator<Storage> ioAccum) {
            logger.trace("construct ttreedatasourcev2reader");
            this.sparkContext = sparkContext;
            try {
                this.paths = new LinkedList<String>();
                for (String path: options.paths()) {
                    this.paths.addAll(IOFactory.expandPathToList(path));
                }
                // FIXME - More than one file, please
                currFile = TFile.getFromFile(fileCache.getROOTFile(this.paths.get(0)));
                treeName = options.get("tree").orElse("Events");
                currTree = new TTree(currFile.getProxy(treeName), currFile);
                this.basketCacheFactory = basketCacheFactory;
                this.schema = readSchemaPriv();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            threadCount = options.getInt("threadCount", 16);

            Function<Event, Integer> cb = null;
            if (ioAccum != null) {
                profileData = ioAccum;
                cb = e -> {
                    profileData.add(e.getStorage());
                    return 0;
                };
            }
            profiler = IOProfile.getInstance(0, cb);
        }

        @Override
        public StructType readSchema() {
            return schema;
        }

        private StructType readSchemaPriv() {
            logger.trace("readschemapriv");
            List<TBranch> branches = currTree.getBranches();
            List<StructField> fields = readSchemaPart(branches, "");
            StructField[] fieldArray = new StructField[fields.size()];
            fieldArray = fields.toArray(fieldArray);
            StructType schema = new StructType(fieldArray);
            return schema;
        }

        private List<StructField> readSchemaPart(List<TBranch> branches, String prefix) {
            List<StructField> fields = new ArrayList<StructField>();
            for (TBranch branch: branches) {
                // The ROOT-given branch name
                String name = branch.getName();
                try {
                    // The name of the "current" level of the branch e.g. what
                    // we would name this StructField
                    String currName = name;
                    if (currName.startsWith(prefix)) {
                        int len = prefix.length();
                        currName = name.substring(len);
                    }
                    if (currName.endsWith(".")) {
                        currName = currName.substring(0, currName.length() - 1);
                    }
                    if (currName.startsWith(".")) {
                        currName = currName.substring(1);
                    }

                    int branchCount = branch.getBranches().size();
                    int leafCount = branch.getLeaves().size();
                    MetadataBuilder metadata = new MetadataBuilder();
                    if (branchCount != 0) {
                        /*
                         * We have sub-branches, so we need to recurse.
                         */
                        String subname = name.substring(prefix.length());
                        List<StructField> subFields = readSchemaPart(branch.getBranches(), name);
                        StructField[] subFieldArray = new StructField[subFields.size()];
                        subFieldArray = subFields.toArray(subFieldArray);
                        StructType subStruct = new StructType(subFieldArray);
                        metadata.putString("rootType", "nested");
                        fields.add(new StructField(currName, subStruct, false, Metadata.empty()));
                    } else if ((branchCount == 0) && (leafCount == 1)) {
                        DataType sparkType = rootToSparkType(branch.getSimpleType());
                        metadata.putString("rootType", branch.getSimpleType().getBaseType().toString());
                        fields.add(new StructField(currName, sparkType, false, metadata.build()));
                    } else {
                        throw new RuntimeException("Unsupported schema for branch " + branch.getName() + " branchCount: " + branchCount + " leafCount: " + leafCount);
                    }
                } catch (UnsupportedBranchTypeException e) {
                    logger.error(String.format("The branch \"%s\" is unable to be deserialized and will be skipped", name));
                }
            }
            return fields;
        }

        private DataType rootToSparkType(SimpleType simpleType) {
            DataType ret = null;
            if (simpleType instanceof SimpleType.ScalarType) {
                if (simpleType == SimpleType.Bool) {
                    ret = DataTypes.BooleanType;
                } else if (simpleType == SimpleType.Int8) {
                    ret = DataTypes.ByteType;
                } else if ((simpleType == SimpleType.Int16) || (simpleType == SimpleType.UInt8)) {
                    ret = DataTypes.ShortType;
                } else if ((simpleType == SimpleType.Int32) || (simpleType == SimpleType.UInt16)) {
                    ret = DataTypes.IntegerType;
                } else if ((simpleType == SimpleType.UInt64) || (simpleType == SimpleType.Int64) || (simpleType == SimpleType.UInt32)) {
                    ret = DataTypes.LongType;
                } else if (simpleType == SimpleType.Float32) {
                    ret = DataTypes.FloatType;
                } else if (simpleType == SimpleType.Float64) {
                    ret = DataTypes.DoubleType;
                } else if (simpleType == SimpleType.Pointer) {
                    ret = DataTypes.LongType;
                }
            } else if (simpleType instanceof SimpleType.ArrayType) {
                SimpleType nested = ((SimpleType.ArrayType) simpleType).getChildType();
                ret = DataTypes.createArrayType(rootToSparkType(nested), false);
            }
            if (ret == null) {
                throw new RuntimeException("Unable to convert ROOT type '" + simpleType + "' to Spark");
            }
            return ret;
        }

        protected static class PartitionHelper implements Serializable {
            private static final long serialVersionUID = 1L;
            /**
             * How many events to put in a partition
             */
            private static final long PARTITION_SIZE = 200 * 1000;
            String treeName;
            StructType schema;
            int threadCount;
            CacheFactory basketCacheFactory;

            public PartitionHelper(String treeName, StructType schema, int threadCount, CacheFactory basketCacheFactory) {
                this.treeName = treeName;
                this.schema = schema;
                this.threadCount = threadCount;
                this.basketCacheFactory = basketCacheFactory;
            }

            private static void parseStructFields(TTree inputTree, Map<String, SlimTBranch> slimBranches, StructType struct, String namespace) {
                for (StructField field: struct.fields())  {
                    if (field.dataType() instanceof StructType) {
                        parseStructFields(inputTree, slimBranches, (StructType) field.dataType(), namespace + field.name() + ".");
                    }
                    ArrayList<TBranch> branchList = inputTree.getBranches(namespace + field.name());
                    assert branchList.size() == 1;
                    TBranch fatBranch = branchList.get(0);
                    SlimTBranch slimBranch = SlimTBranch.getFromTBranch(fatBranch);
                    slimBranches.put(fatBranch.getName(), slimBranch);
                }
            }

            public static Iterator<InputPartition<ColumnarBatch>> partitionSingleFileImpl(String path, String treeName, StructType schema, int threadCount, CacheFactory basketCacheFactory) {
                List<InputPartition<ColumnarBatch>> ret = new ArrayList<InputPartition<ColumnarBatch>>();
                int pid = 0;
                TTree inputTree;

                try {
                    TFile inputFile = TFile.getFromFile(fileCache.getROOTFile(path));
                    inputTree = new TTree(inputFile.getProxy(treeName), inputFile);

                    Map<String, SlimTBranch> slimBranches = new HashMap<String, SlimTBranch>();
                    parseStructFields(inputTree, slimBranches, schema, "");

                    // TODO We partition based on a fixed number of events per
                    //      partition, which isn't smart. Redo it with something
                    //      smarter later
                    long[] entryOffset = inputTree.getBranches().get(0).getBasketEntryOffsets();
                    long lastEntry = entryOffset[entryOffset.length - 1];
                    for (int i = 0; i < lastEntry; i += PARTITION_SIZE) {
                        pid += 1;
                        long partitionStart = i;
                        long partitionEnd = Math.min(lastEntry, partitionStart + PARTITION_SIZE);
                        Map<String, SlimTBranch> trimmedSlimBranches = new HashMap<String, SlimTBranch>();
                        for (Entry<String, SlimTBranch> e: slimBranches.entrySet()) {
                            trimmedSlimBranches.put(e.getKey(), e.getValue().copyAndTrim(partitionStart, partitionEnd));
                        }
                        ret.add(new TTreeDataSourceV2Partition(schema, basketCacheFactory, partitionStart, partitionEnd, trimmedSlimBranches, threadCount, profileData, pid));
                    }
                    if (ret.size() == 0) {
                        // Only one basket?
                        logger.debug("Planned for zero baskets, adding a dummy one");
                        pid += 1;
                        ret.add(new TTreeDataSourceV2Partition(schema, basketCacheFactory, 0, inputTree.getEntries(), slimBranches, threadCount, profileData, pid));
                    }
                    return ret.iterator();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }

            FlatMapFunction<String, InputPartition<ColumnarBatch>> getLambda() {
                return s -> PartitionHelper.partitionSingleFileImpl(s, treeName, schema, threadCount, basketCacheFactory);
            }
        }

        @Override
        public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
            logger.trace("planbatchinputpartitions");
            List<InputPartition<ColumnarBatch>> ret = new ArrayList<InputPartition<ColumnarBatch>>();
            if (sparkContext == null) {
                for (String path: paths) {
                    partitionSingleFile(path).forEachRemaining(ret::add);;
                }
            } else {
                JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
                JavaRDD<String> rdd_paths = sc.parallelize(paths, paths.size());
                PartitionHelper helper = new PartitionHelper(treeName, schema, threadCount, basketCacheFactory);
                JavaRDD<InputPartition<ColumnarBatch>> partitions = rdd_paths.flatMap(helper.getLambda());
                ret = partitions.collect();
            }
            int pid = 0;
            for (InputPartition<ColumnarBatch> x: ret) {
                ((TTreeDataSourceV2Partition)x).setPid(pid);
                pid += 1;
            }
            return ret;
        }

        public Iterator<InputPartition<ColumnarBatch>> partitionSingleFile(String path) {
            return PartitionHelper.partitionSingleFileImpl(path, treeName, schema, threadCount, basketCacheFactory);
        }

        @Override
        public void pruneColumns(StructType requiredSchema) {
            logger.trace("prunecolumns ");
            schema = requiredSchema;
        }

    }

    /**
     * Accumulator for IOProfiling information
     */
    private static CollectionAccumulator<Event.Storage> ioAccum = null;

    /**
     * In Spark2.X, datasource objects have a very .. interesting lifetime where
     * they're created every time an antecedent operation needs one. Since we
     * do a lot of work to initialize the reader (loading all the files, etc..),
     * this is very unperformant
     *
     * <p>Utilize a cache where the key is the DataSourceOptions and the value
     * is the actual reader object. Unfortunately, since the readers themselves
     * are transient, we need to make these soft references so they're not
     * eagerly garbage collected
     */
    private static LoadingCache<DataSourceReaderKey,
                                DataSourceReader> dedupDataSource =
                                    CacheBuilder.newBuilder()
                                    .softValues()
                                    .maximumSize(100)
                                    .build(
                                       new CacheLoader<DataSourceReaderKey,
                                                       DataSourceReader>() {
                                            @Override
                                            public DataSourceReader load(DataSourceReaderKey key) {
                                                logger.trace("Construct new reader");
                                                DataSourceOptions options = new DataSourceOptions(key.options);
                                                boolean traceIO = key.traceIO;
                                                SparkContext context = key.context;
                                                CacheFactory basketCacheFactory = new CacheFactory();
                                                if ((traceIO) && (context != null)) {
                                                    synchronized (Root.class) {
                                                        ioAccum = new CollectionAccumulator<Event.Storage>();
                                                        context.register(ioAccum, "edu.vanderbilt.accre.laurelin.ioprofile");
                                                    }
                                                }
                                                return new TTreeDataSourceV2Reader(options, basketCacheFactory, context, ioAccum);
                                            }
                                            });

    static class DataSourceReaderKey {
        @Override
        public int hashCode() {
            return Objects.hash(context, options, traceIO);
        }

        @Override
        public boolean equals(Object obj) {
            DataSourceReaderKey other = (DataSourceReaderKey) obj;
            return (context == other.context) && options.equals(other.options)
                    && traceIO == other.traceIO;
        }

        public DataSourceReaderKey(DataSourceOptions options, SparkContext context, boolean traceIO) {
            this.traceIO = traceIO;
            this.context = context;
            this.options = options.asMap();

        }

        boolean traceIO;
        SparkContext context;
        Map<String, String> options;
    }

    /**
     * This is called by Spark, unlike the following function that accepts a
     * SparkContext
     * @param options DS options
     */
    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return createReader(options, SparkContext.getOrCreate(), false);
    }

    /**
     * Used for unit-tests when there is no current spark context
     * @param options DS options
     * @param context spark context to use
     * @param traceIO whether or not to trace the IO operations
     * @return new reader
     */
    public DataSourceReader createReader(DataSourceOptions options, SparkContext context, boolean traceIO) {
        try {
            return dedupDataSource.get(new DataSourceReaderKey(options, context, traceIO));
        } catch (ExecutionException e) {
            throw new RuntimeException("Could not load DataSourceReader", e);
        }
    }

    @Override
    public String shortName() {
        return "root";
    }

}
