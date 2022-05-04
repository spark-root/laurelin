package edu.vanderbilt.accre.laurelin.spark_ttree;

import static edu.vanderbilt.accre.laurelin.Helpers.getBigTestDataIfExists;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.Root;
import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;

public class ParseSchemaTest {
    @Test
    public void testGetSchemaNano() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/nano_tree.root");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1011, schemaCast.size());
    }

    /**
     * Only test if we have the big 2016 nanoaod file downloaded
     */
    @Test
    public void testGetSchemaBigNano() {
        String testPath = getBigTestDataIfExists("testdata/A2C66680-E3AA-E811-A854-1CC1DE192766.root");
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(866, schemaCast.size());
    }

    @Test
    public void testGetSchema100MBNano() {
        String testPath = getBigTestDataIfExists("testdata/nano_19.root");
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1119, schemaCast.size());
    }

    @Test
    public void testGetSchemaForiter() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-foriter.root");
        optmap.put("tree",  "foriter");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1, schemaCast.size());
    }


    @Test
    public void testGetSchemaFlat() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
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
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        //System.out.println(schema.prettyJson());
        assertEquals(1, schemaCast.size());
    }

}
