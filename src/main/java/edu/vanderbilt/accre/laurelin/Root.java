package edu.vanderbilt.accre.laurelin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TLeaf;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch.SlimTBasket;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeColumnVector;

public class Root implements DataSourceV2, ReadSupport {
    // InputPartition is serialized and sent to the executor who then makes the
    // InputPartitionReader, I don't think we need to have that division
    private static final Logger logger = LogManager.getLogger();

    /**
     * Represents a Partition of a TTree, which currently is one per-file.
     * Future improvements will split this up per-basket. Big files = big mem
     * usage!
     *
     * This is instantiated on the driver, then serialized and transmitted to
     * the executor
     */
    static class TTreeDataSourceV2Partition implements InputPartition<ColumnarBatch> {
        private static final long serialVersionUID = -6598704946339913432L;
        private StructType schema;
        private long entryStart;
        private long entryEnd;
        private Map<String, SlimTBranch> slimBranches;

        private CacheFactory basketCacheFactory;


        public TTreeDataSourceV2Partition(StructType schema, CacheFactory basketCacheFactory, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches) {
            logger.trace("dsv2partition new");
            this.schema = schema;
            this.basketCacheFactory = basketCacheFactory;
            this.entryStart = entryStart;
            this.entryEnd = entryEnd;
            this.slimBranches = slimBranches;
        }

        /*
         * Begin InputPartition overrides
         */

        @Override
        public InputPartitionReader<ColumnarBatch> createPartitionReader() {
            logger.trace("input partition reader");
            return new TTreeDataSourceV2PartitionReader(basketCacheFactory, schema, entryStart, entryEnd, slimBranches);
        }
    }

    static class TTreeDataSourceV2PartitionReader implements InputPartitionReader<ColumnarBatch> {
        private Cache basketCache;
        private StructType schema;
        private long entryStart;
        private long entryEnd;
        private int currBasket = -1;
        private Map<String, SlimTBranch> slimBranches;
        public TTreeDataSourceV2PartitionReader(CacheFactory basketCacheFactory, StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches) {
            this.basketCache = basketCacheFactory.getCache();
            this.schema = schema;
            this.entryStart = entryStart;
            this.entryEnd = entryEnd;
            this.slimBranches = slimBranches;
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
            ColumnVector[] vecs = new ColumnVector[schema.fields().length];
            int idx = 0;
            for (StructField field: schema.fields())  {
                if (field.dataType() instanceof StructType) {
                    throw new RuntimeException("Nested fields are not supported: " + field.name());
                }
                SlimTBranch slimBranch = slimBranches.get(field.name());
                vecs[idx] = new TTreeColumnVector(field.dataType(), basketCache, entryStart, entryEnd - entryStart, slimBranch);
                idx += 1;
            }
            ColumnarBatch ret = new ColumnarBatch(vecs);
            ret.setNumRows((int) (entryEnd - entryStart));
            return ret;
        }
    }

    public static class TTreeDataSourceV2Reader implements DataSourceReader,
    SupportsScanColumnarBatch,
    SupportsPushDownRequiredColumns {
        private String[] paths;
        private String treeName;
        private TTree currTree;
        private TFile currFile;
        private CacheFactory basketCacheFactory;
        private StructType schema;

        public TTreeDataSourceV2Reader(DataSourceOptions options, CacheFactory basketCacheFactory) {
            logger.trace("construct ttreedatasourcev2reader");
            try {
                this.paths = options.paths();
                // FIXME - More than one file, please
                currFile = TFile.getFromFile(this.paths[0]);
                treeName = options.get("tree").orElse("Events");
                currTree = new TTree(currFile.getProxy(treeName), currFile);
                this.basketCacheFactory = basketCacheFactory;
                this.schema = readSchemaPriv();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
            for (TBranch branch: branches ) {
                if (branch.getBranches().size() != 0) {
                    List<StructField> subFields = readSchemaPart(branch.getBranches(), prefix);
                    StructField[] subFieldArray = new StructField[subFields.size()];
                    subFieldArray = subFields.toArray(subFieldArray);
                    StructType subStruct = new StructType(subFieldArray);
                    fields.add(new StructField(branch.getName(), subStruct, false, Metadata.empty()));
                } else {
                    if (branch.getLeaves().size() > 1) {
                        throw new RuntimeException("Un-split branches are not supported. Current branch: " + branch.getName());
                    } else {
                        TLeaf leaf = branch.getLeaves().get(0);
                        DataType sparkType = rootToSparkType(leaf.getSimpleType());
                        fields.add(new StructField(leaf.getName(), sparkType, false, Metadata.empty()));
                    }
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
            for (String path: paths) {
                Map<String, SlimTBranch> slimBranches = new HashMap<String, SlimTBranch>();
                for (StructField field: schema.fields())  {
                    if (field.dataType() instanceof StructType) {
                        throw new RuntimeException("Nested fields are not supported: " + field.name());
                    }
                    ArrayList<TBranch> branchList = currTree.getBranches(field.name());
                    assert branchList.size() == 1;
                    TBranch fatBranch = branchList.get(0);
                    long[] entryOffset = fatBranch.getBasketEntryOffsets();
                    SlimTBranch slimBranch = new SlimTBranch(path, entryOffset, fatBranch.getArrayDescriptor());
                    slimBranches.put(field.name(), slimBranch);
                    for (TBasket basket: fatBranch.getBaskets()) {
                        SlimTBasket slimBasket = new SlimTBasket(slimBranch,
                                                                    basket.getAbsoluteOffset(),
                                                                    basket.getBasketBytes() - basket.getKeyLen(),
                                                                    basket.getObjLen(),
                                                                    basket.getKeyLen(),
                                                                    basket.getLast()
                                                                    );
                        slimBranch.addBasket(slimBasket);
                    }

                }


                // TODO We partition based on the basketing of the first branch
                //      which might not be optimal. We should do something
                //      smarter later
                long[] entryOffset = currTree.getBranches().get(0).getBasketEntryOffsets();
                for (int i = 0; i < (entryOffset.length - 1); i += 1) {
                    long entryStart = entryOffset[i];
                    long entryEnd = entryOffset[i+1];
                    // the last basket is dumb and annoying
                    if (i == (entryOffset.length - 1)) {
                        entryEnd = currTree.getEntries();
                    }

                    ret.add(new TTreeDataSourceV2Partition(schema, basketCacheFactory, entryStart, entryEnd, slimBranches));
                }
            }
            return ret;
        }

        @Override
        public void pruneColumns(StructType requiredSchema) {
            logger.trace("prunecolumns");
            schema = requiredSchema;
        }

    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        logger.trace("make new reader");
        CacheFactory basketCacheFactory = new CacheFactory();
        return new TTreeDataSourceV2Reader(options, basketCacheFactory);
    }

}
