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
         df = df.select("Float32", "ArrayFloat32");
         df.show();
         assertEquals(100, df.count());
     }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

}
