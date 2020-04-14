package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.ROOTException.UnsupportedBranchTypeException;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOFactory;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFileCache;

/**
 * Backing object for a collection of root files
 * <p>
 * Used by Reader_v24 and Table_v30
 */
public class Reader {
    static final Logger logger = LogManager.getLogger();

    private List<String> paths;
    private String treeName;
    private StructType schema;
    private int threadCount;
    private IOProfile profiler;
    private static CollectionAccumulator<Storage> profileData;
    private SparkContext sparkContext;
    private static ROOTFileCache fileCache = ROOTFileCache.getCache();

    public Reader(List<String> userPaths, DataSourceOptionsInterface options, SparkContext sparkContext, CollectionAccumulator<Storage> ioAccum) {
        logger.trace("construct ttreedatasourcev2reader");
        this.sparkContext = sparkContext;
        try {
            List<Path> expanded = IOFactory.resolvePathList(options.getPaths());
            this.paths = new ArrayList<String>(expanded.size());
            for (Path p: expanded) {
                this.paths.add(p.toString());
            }
            // FIXME - More than one file, please
            treeName = options.getOrDefault("tree", "Events");
            this.schema = getSchemaFromFiles(userPaths, options);
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

    public StructType readSchema() {
        return schema;
    }

    public static StructType getSchemaFromFiles(List<String> userPaths, DataSourceOptionsInterface options)  {
        try {
            TFile file = TFile.getFromFile(fileCache.getROOTFile(userPaths.get(0)));
            String name = options.getOrDefault("tree", "Events");
            TTree tree = new TTree(file.getProxy(name), file);
            List<TBranch> branches = tree.getBranches();
            List<StructField> fields = readSchemaPart(branches, "");
            StructField[] fieldArray = new StructField[fields.size()];
            fieldArray = fields.toArray(fieldArray);
            StructType schema = new StructType(fieldArray);
            return schema;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static List<StructField> readSchemaPart(List<TBranch> branches, String prefix) {
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

    private static DataType rootToSparkType(SimpleType simpleType) {
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

        public PartitionHelper(String treeName, StructType schema, int threadCount) {
            this.treeName = treeName;
            this.schema = schema;
            this.threadCount = threadCount;
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

        public static Iterator<Partition> partitionSingleFileImpl(String path, String treeName, StructType schema, int threadCount) {
            List<Partition> ret = new ArrayList<Partition>();
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
                    ret.add(new Partition(schema, partitionStart, partitionEnd, trimmedSlimBranches, threadCount, profileData, pid));
                }
                if (ret.size() == 0) {
                    // Only one basket?
                    logger.debug("Planned for zero baskets, adding a dummy one");
                    pid += 1;
                    ret.add(new Partition(schema, 0, inputTree.getEntries(), slimBranches, threadCount, profileData, pid));
                }
                return ret.iterator();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        FlatMapFunction<String, Partition> getLambda() {
            return s -> PartitionHelper.partitionSingleFileImpl(s, treeName, schema, threadCount);
        }
    }

    public List<Partition> planBatchInputPartitions() {
        logger.trace("planbatchinputpartitions");
        List<Partition> ret = new ArrayList<Partition>();
        if (sparkContext == null) {
            for (String path: paths) {
                partitionSingleFile(path).forEachRemaining(ret::add);;
            }
        } else {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkContext);
            JavaRDD<String> rdd_paths = sc.parallelize(paths, paths.size());
            Reader.PartitionHelper helper = new PartitionHelper(treeName, schema, threadCount);
            JavaRDD<Partition> partitions = rdd_paths.flatMap(helper.getLambda());
            ret = partitions.collect();
        }
        int pid = 0;
        for (Partition x: ret) {
            x.setPid(pid);
            pid += 1;
        }
        return ret;
    }

    public Iterator<Partition> partitionSingleFile(String path) {
        return PartitionHelper.partitionSingleFileImpl(path, treeName, schema, threadCount);
    }

    public void pruneColumns(StructType requiredSchema) {
        logger.trace("prunecolumns ");
        schema = requiredSchema;
    }
}
