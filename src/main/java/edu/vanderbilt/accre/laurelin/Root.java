package edu.vanderbilt.accre.laurelin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
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

import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;
import edu.vanderbilt.accre.laurelin.root_proxy.IOFactory;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.StructColumnVector;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeColumnVector;

public class Root implements DataSourceV2, ReadSupport, DataSourceRegister {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Represents a Partition of a TTree, which currently is one per-file.
     * Future improvements will split this up per-basket. Big files = big mem
     * usage!
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
    }

    static class TTreeDataSourceV2PartitionReader implements InputPartitionReader<ColumnarBatch> {
        private Cache basketCache;
        private StructType schema;
        private long entryStart;
        private long entryEnd;
        private int currBasket = -1;
        private Map<String, SlimTBranch> slimBranches;
        private static ThreadPoolExecutor executor;
        private CollectionAccumulator<Storage> profileData;
        private int pid;

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
                executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);
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
                SlimTBranch slimBranch = slimBranches.get(field.name());
                SimpleType rootType;
                rootType = SimpleType.fromString(field.metadata().getString("rootType"));

                Dtype dtype = SimpleType.dtypeFromString(field.metadata().getString("rootType"));
                vecs.add(new TTreeColumnVector(field.dataType(), rootType, dtype, basketCache, entryStart, entryEnd - entryStart, slimBranch, executor));
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

        public TTreeDataSourceV2Reader(DataSourceOptions options, CacheFactory basketCacheFactory, SparkContext sparkContext, CollectionAccumulator<Storage> ioAccum) {
            logger.trace("construct ttreedatasourcev2reader");
            try {
                this.paths = new LinkedList<String>();
                for (String path: options.paths()) {
                    this.paths.addAll(IOFactory.expandPathToList(path));
                }
                // FIXME - More than one file, please
                currFile = TFile.getFromFile(this.paths.get(0));
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
                int branchCount = branch.getBranches().size();
                int leafCount = branch.getLeaves().size();
                MetadataBuilder metadata = new MetadataBuilder();
                if (branchCount != 0) {
                    /*
                     * We have sub-branches, so we need to recurse.
                     */
                    List<StructField> subFields = readSchemaPart(branch.getBranches(), prefix);
                    StructField[] subFieldArray = new StructField[subFields.size()];
                    subFieldArray = subFields.toArray(subFieldArray);
                    StructType subStruct = new StructType(subFieldArray);
                    metadata.putString("rootType", "nested");
                    fields.add(new StructField(branch.getName(), subStruct, false, Metadata.empty()));
                } else if ((branchCount == 0) && (leafCount == 1)) {
                    DataType sparkType = rootToSparkType(branch.getSimpleType());
                    metadata.putString("rootType", branch.getSimpleType().getBaseType().toString());
                    fields.add(new StructField(branch.getName(), sparkType, false, metadata.build()));
                } else {
                    throw new RuntimeException("Unsupported schema for branch " + branch.getName() + " branchCount: " + branchCount + " leafCount: " + leafCount);
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
                } else if ((simpleType == SimpleType.Int64) || (simpleType == SimpleType.UInt32)) {
                    ret = DataTypes.LongType;
                } else if (simpleType == SimpleType.Float32) {
                    ret = DataTypes.FloatType;
                } else if ((simpleType == SimpleType.Float64) || (simpleType == SimpleType.UInt64)) {
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

        @Override
        public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
            logger.trace("planbatchinputpartitions");
            List<InputPartition<ColumnarBatch>> ret = new ArrayList<InputPartition<ColumnarBatch>>();
            int pid = 0;
            for (String path: paths) {
                TTree inputTree;
                try {
                    TFile inputFile = TFile.getFromFile(path);
                    inputTree = new TTree(currFile.getProxy(treeName), inputFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                Map<String, SlimTBranch> slimBranches = new HashMap<String, SlimTBranch>();
                parseStructFields(inputTree, slimBranches, schema, "");


                // TODO We partition based on the basketing of the first branch
                //      which might not be optimal. We should do something
                //      smarter later
                long[] entryOffset = inputTree.getBranches().get(0).getBasketEntryOffsets();
                for (int i = 0; i < (entryOffset.length - 1); i += 1) {
                    pid += 1;
                    long entryStart = entryOffset[i];
                    long entryEnd = entryOffset[i + 1];
                    // the last basket is dumb and annoying
                    if (i == (entryOffset.length - 1)) {
                        entryEnd = inputTree.getEntries();
                    }

                    ret.add(new TTreeDataSourceV2Partition(schema, basketCacheFactory, entryStart, entryEnd, slimBranches, threadCount, profileData, pid));
                }
                if (ret.size() == 0) {
                    // Only one basket?
                    logger.debug("Planned for zero baskets, adding a dummy one");
                    pid += 1;
                    ret.add(new TTreeDataSourceV2Partition(schema, basketCacheFactory, 0, inputTree.getEntries(), slimBranches, threadCount, profileData, pid));
                }
            }
            return ret;
        }

        private void parseStructFields(TTree inputTree, Map<String, SlimTBranch> slimBranches, StructType struct, String namespace) {
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

        @Override
        public void pruneColumns(StructType requiredSchema) {
            logger.trace("prunecolumns ");
            schema = requiredSchema;
        }

    }

    /*
     * This is called by Spark, unlike the following function that accepts a
     * SparkContext
     */

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return createReader(options, SparkContext.getOrCreate());
    }

    /**
     * Used for unit-tests when there is no current spark context
     * @param options DS options
     * @param context spark context to use
     * @return new reader
     */

    public DataSourceReader createReader(DataSourceOptions options, SparkContext context) {
        logger.trace("make new reader");
        CacheFactory basketCacheFactory = new CacheFactory();
        CollectionAccumulator<Event.Storage> ioAccum = null;
        if (context != null) {
            ioAccum = new CollectionAccumulator<Event.Storage>();
            context.register(ioAccum, "edu.vanderbilt.accre.laurelin.ioprofile");
        }
        return new TTreeDataSourceV2Reader(options, basketCacheFactory, context, ioAccum);
    }


    @Override
    public String shortName() {
        return "root";
    }

}