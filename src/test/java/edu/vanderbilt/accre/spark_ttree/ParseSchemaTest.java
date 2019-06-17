package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.Root;
import edu.vanderbilt.accre.laurelin.Root.TTreeDataSourceV2Reader;

public class ParseSchemaTest {
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

    @Test
    public void testGetSchemaForiter() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-foriter.root");
        optmap.put("tree",  "foriter");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1, schemaCast.size());
    }


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
    public void testGetSchemaNested() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-nesteddirs.root");
        optmap.put("tree",  "three/tree");
        DataSourceOptions opts = new DataSourceOptions(optmap);
        Root source = new Root();
        TTreeDataSourceV2Reader reader = (TTreeDataSourceV2Reader) source.createReader(opts);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        System.out.println(schema.prettyJson());
        assertEquals(1, schemaCast.size());
    }

}
