package edu.vanderbilt.accre.spark_ttree;

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
        df.printSchema();
        //df.select("Int32", "Int64", "Float32", "Float64").show(2);
        //df.select("ArrayInt32", "ArrayInt64", "ArrayFloat32", "ArrayFloat64").show(2);
        //df.select("SliceInt32", "SliceFloat32", "SliceFloat64").show(2);
        df.select("ArrayFloat32").show(2);
        df.select("SliceFloat32", "SliceInt32").show(2);
        df.select("SliceFloat64", "SliceInt64").show(2);

        //df.select("SliceInt32", "SliceInt64", "SliceFloat32", "SliceFloat64").show(2);

        // assertEquals(100, df.count());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

}
