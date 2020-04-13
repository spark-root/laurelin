package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TTreeDataSourceIntegrationTest {
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("hadoop.home.dir", "/");
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("test").getOrCreate();
    }

    @Test
    public void testLoadDataFrame() {
        Dataset<Row> df = spark
                .read()
                .format("edu.vanderbilt.accre.laurelin.Root")
                .option("tree",  "tree")
                .load("testdata/uproot-small-flat-tree.root");
        df = df.select("Float32", "ArrayFloat32", "SliceFloat32");
        df.show();
        assertEquals(100, df.count());
    }

    @Test
    public void testLoadNestedDataFrame() {
        Dataset<Row> df = spark
                .read()
                .format("edu.vanderbilt.accre.laurelin.Root")
                .option("tree",  "Events")
                .load("testdata/all-types.root");
        df.printSchema();
        df.select("ScalarI8", "ScalarI16", "ScalarI32", "ScalarI64").show();
        df.select("ArrayI8", "ArrayI16", "ArrayI32", "ArrayI64").show();
        df.select("SliceI8", "SliceI16", "SliceI32", "SliceI64").show();
    }

    @Test
    public void testShortName() {
        Dataset<Row> df = spark
                .read()
                .format("root")
                .option("tree",  "Events")
                .option("threadCount", "0")
                .load("testdata/all-types.root");
        df.printSchema();
        df.select("ScalarI8").show();
    }

    @Test
    public void testVectorLoad() {
        // try to load an std::vector
        Dataset<Row> df = spark
                .read()
                .format("root")
                .option("tree",  "tvec")
                .option("threadCount", "0")
                .load("testdata/stdvector.root");
        df.printSchema();
        df.show(false);
    }

    @Test
    public void testTwoFiles() {
        // If the files are duplicated in the input list, they shouldn't be
        // after reading
        Dataset<Row> df = spark
                .read()
                .format("root")
                .option("tree",  "Events")
                .option("threadCount", "0")
                .load("testdata/all-types.root", "testdata/all-types.root");
        assertEquals(9, df.count());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

}
