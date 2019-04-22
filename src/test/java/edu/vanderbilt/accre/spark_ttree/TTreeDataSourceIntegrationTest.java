package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import java.io.File;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TTreeDataSourceIntegrationTest {
	private static SparkSession spark;
	
	@BeforeClass
	public static void beforeClass() {
		spark = SparkSession.builder()
				.master("local[*]")
				.appName("test").getOrCreate();
	}

//	@Test
//	public void testCreateReader() {
//		spark
//          .read()
//          .format("edu.vanderbilt.accre.spark_ttree.TTreeDataSourceV2");
//	}
	
//	@Test
//	public void testLoadFile() {
//		spark
//          .read()
//          .format("edu.vanderbilt.accre.spark_ttree.TTreeDataSourceV2")
//          .load("testdata/uproot-small-flat-tree.root");
//	}
	
	@AfterClass
	public static void afterClass() {
		if (spark != null) {
			//spark.stop();
		}
	}

}
