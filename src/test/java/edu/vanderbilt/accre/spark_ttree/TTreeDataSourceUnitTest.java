package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.Root;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.Root.TTreeDataSourceV2Reader;
import edu.vanderbilt.accre.laurelin.spark_ttree.ArrayColumnVector;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeColumnVector;

public class TTreeDataSourceUnitTest {
    @Test
    public void testIntegers() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceI32").get(0);
        Cache cache = new Cache();
        SlimTBranch slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new IntegerType(), false), new SimpleType.ArrayType(SimpleType.fromString("int")), SimpleType.dtypeFromString("int"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        System.out.println(String.format("[]"));
        ColumnarArray event1 = result.getArray(1);
        System.out.println(String.format("[%d]", event1.getInt(0)));
        ColumnarArray event2 = result.getArray(2);
        System.out.println(String.format("[%d, %d]", event2.getInt(0), event2.getInt(1)));
        ColumnarArray event3 = result.getArray(3);
        ColumnarArray event4 = result.getArray(4);
        ColumnarArray event5 = result.getArray(5);
        ColumnarArray event6 = result.getArray(6);
        ColumnarArray event7 = result.getArray(7);
        ColumnarArray event8 = result.getArray(8);
    }

    @Test
    public void testLoadNestedDataFrame() {
        System.setProperty("hadoop.home.dir", "/");
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("test").getOrCreate();
        Dataset<Row> df = spark
                .read()
                .format("edu.vanderbilt.accre.laurelin.Root")
                .option("tree", "Events")
                .option("threadCount", "1")
                .load("testdata/all-types.root");
        df.select("SliceI32").show();
    }

    @Test
    public void testMultipleBasketsForiter() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-foriter.root");
        optmap.put("tree",  "foriter");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1, schemaCast.size());
        List<InputPartition<ColumnarBatch>> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(8, partitions.size());
        InputPartition<ColumnarBatch> partition;
        long []expectedCounts = {6, 6, 6, 6, 6, 6, 6, 4};
        for (int i = 0; i < 8; i += 1) {
            partition = partitions.get(i);
            InputPartitionReader<ColumnarBatch> partitionReader = partition.createPartitionReader();
            assertTrue(partitionReader.next());
            ColumnarBatch batch = partitionReader.get();
            assertFalse(partitionReader.next());
            assertEquals(expectedCounts[i], batch.numRows());
        }
    }

    @Test
    public void testMultipleBasketsForBigNano() throws IOException {
        String testPath = "testdata/A2C66680-E3AA-E811-A854-1CC1DE192766.root";
        File f = new File(testPath);
        assumeTrue(f.isFile());
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        optmap.put("tree",  "Events");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        // only get a scalar float_t for now since that's all that works
        MetadataBuilder metadata = new MetadataBuilder();
        metadata.putString("rootType", "float");
        StructType prune = new StructType()
                            .add(new StructField("CaloMET_pt", DataTypes.FloatType, false, metadata.build()));
        reader.pruneColumns(prune);
        List<InputPartition<ColumnarBatch>> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(13, partitions.size());
        InputPartition<ColumnarBatch> partition;
        long []expectedCounts = {1000,
                                    13519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    14519,
                                    1827
                                 };
        for (int i = 0; i < 8; i += 1) {
            partition = partitions.get(i);
            InputPartitionReader<ColumnarBatch> partitionReader = partition.createPartitionReader();
            assertTrue(partitionReader.next());
            ColumnarBatch batch = partitionReader.get();
            assertFalse(partitionReader.next());
            assertEquals(expectedCounts[i], batch.numRows());
        }
    }



    //@Test(expected = IllegalStateException.class)
    @Test
    public void testPlanInputPartitions() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        List<InputPartition<ColumnarBatch>> batch = reader.planBatchInputPartitions();
        assertNotNull(batch);
    }

    @Test
    public void testPlanBatchInputPartitions() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        assertNotNull(reader.planBatchInputPartitions());
    }

    /**
     * Ideally implements the same call order as the full-up spark test
     *  [TRACE] 17:24:35.974 e.v.a.l.Root - planbatchinputpartitions
     *  [TRACE] 17:24:35.974 e.v.a.l.Root - readschema
     *  [TRACE] 17:24:35.975 e.v.a.l.Root - dsv2partition new
     *  [TRACE] 17:24:36.093 e.v.a.l.Root - input partition reader
     *  [INFO ] 17:24:36.096 e.v.a.l.r.TTree - Ignoring unparsable/empty branch "Str"
     *  [TRACE] 17:24:36.098 e.v.a.l.Root - next
     *  [TRACE] 17:24:36.098 e.v.a.l.Root - columnarbatch
     *  [TRACE] 17:24:36.107 e.v.a.l.Root - close
     * @throws IOException
     */
    @Test
    public void testLoadScalarFloat32() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        List<InputPartition<ColumnarBatch>> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        StructType schema = reader.readSchema();


        InputPartition<ColumnarBatch> partition = partitions.get(0);
        InputPartitionReader<ColumnarBatch> partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        assertFalse(partitionReader.next());
        // 19 branches in this file
        assertEquals(19, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("Float32").get());
        assertEquals(0.0f, float32col.getFloat(0), 0.001);
        assertEquals(99.0f, float32col.getFloat(99), 0.001);

        assertFloatArrayEquals(new float[] { 0.0f, 1.0f }, float32col.getFloats(0, 2));
        assertFloatArrayEquals(new float[] { 10.0f, 11.0f }, float32col.getFloats(10, 2));
        assertFloatArrayEquals(new float[] { 22.0f, 23.0f, 24.0f }, float32col.getFloats(22, 3));
    }

    private void assertFloatArrayEquals(float exp[], float act[]) {
        assertEquals("Arrays same length", exp.length, act.length);
        for (int i = 0; i < exp.length; i += 1) {
            assertEquals("Array index " + i + " mismatched", exp[i], act[i], 0.001);
        }
    }

    @Test
    public void testLoadFixedArrayFloat32() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        List<InputPartition<ColumnarBatch>> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        StructType schema = reader.readSchema();

        InputPartition<ColumnarBatch> partition = partitions.get(0);
        InputPartitionReader<ColumnarBatch> partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        assertFalse(partitionReader.next());
        // 19 branches in this file
        assertEquals(19, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("ArrayFloat32").get());

        assertFloatArrayEquals(new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, float32col.getArray(0).toFloatArray());
        assertFloatArrayEquals(new float[] { 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f}, float32col.getArray(10).toFloatArray());
        assertFloatArrayEquals(new float[] { 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f}, float32col.getArray(31).toFloatArray());
    }

}
