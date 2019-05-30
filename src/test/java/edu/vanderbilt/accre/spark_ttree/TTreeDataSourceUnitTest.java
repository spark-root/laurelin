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

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.Root;
import edu.vanderbilt.accre.laurelin.Root.TTreeDataSourceV2Reader;

public class TTreeDataSourceUnitTest {

    @Test
    public void testGetSchemaFlat() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(20, schemaCast.size());
    }

    @Test
    public void testGetSchemaNano() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/nano_tree.root");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1011, schemaCast.size());
    }

    /**
     * Only test if we have the big 2016 nanoaod file downloaded
     */
    @Test
    public void testGetSchemaBigNano() {
        String testPath = "testdata/A2C66680-E3AA-E811-A854-1CC1DE192766.root";
        File f = new File(testPath);
        assumeTrue(f.isFile());
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(866, schemaCast.size());
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
        // 20 branches in this file
        assertEquals(20, batch.numCols());
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
        // 20 branches in this file
        assertEquals(20, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("ArrayFloat32").get());

        assertFloatArrayEquals(new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, float32col.getArray(0).toFloatArray());
        assertFloatArrayEquals(new float[] { 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f}, float32col.getArray(10).toFloatArray());
        assertFloatArrayEquals(new float[] { 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f}, float32col.getArray(31).toFloatArray());
    }

    @Test
    public void testLoadScalarString() throws IOException {
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
        // 20 branches in this file
        assertEquals(20, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("Str").get());
        assertEquals("Str", float32col.getUTF8String(0));
    }

    //	@Test
    //	public void testPlanPruneColumns() {
    //		Map<String, String> optmap = new HashMap<String, String>();
    //		DataSourceOptions opts = new DataSourceOptions(optmap);
    //		TTreeDataSourceV2 source = new TTreeDataSourceV2();
    //		TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
    //		StructType schema = new StructType();
    //		schema.add("Float32", DataTypes.ByteType);
    //		reader.pruneColumns(schema);
    //	}


}
