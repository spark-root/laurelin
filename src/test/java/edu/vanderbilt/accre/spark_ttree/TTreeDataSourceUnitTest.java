package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
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
        // Note - there's 20 branches, but we ignore one because I'm not trying to deserialize strings
        assertEquals(19, schemaCast.size());
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
