package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;
// see sql/core/src/test/java/test/org/apache/spark/sql/connector/JavaSimpleDataSourceV2.java
// FileDataSourceV2
/**
 * Top-level entrypoint from Spark into Laurelin
 */
public class Root_v30 implements TableProvider, DataSourceRegister {
    static final Logger logger = LogManager.getLogger();

    @Override
    public String shortName() {
        return "root";
    }

    public Reader createTestReader(LaurelinDSConfig options, SparkContext context, boolean traceIO) {
        List<String> paths = options.paths();
        return new Reader(paths, options, context);
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        LaurelinDSConfig optionsUpcast = LaurelinDSConfig.wrap(options.asCaseSensitiveMap());
        List<String> paths = optionsUpcast.paths();
        return Reader.getSchemaFromFiles(paths, optionsUpcast);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        LaurelinDSConfig optionsUpcast = LaurelinDSConfig.wrap(properties);
        List<String> paths = optionsUpcast.paths();
        return new Table_v30(optionsUpcast, paths);
    }

}