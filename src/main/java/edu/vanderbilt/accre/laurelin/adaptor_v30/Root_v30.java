package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.util.Collections;
import java.util.LinkedList;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.vanderbilt.accre.laurelin.Root.DataSourceOptionsAdaptor;
import edu.vanderbilt.accre.laurelin.spark_ttree.DataSourceOptionsInterface;
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

    private List<String> getPathsPriv(CaseInsensitiveStringMap options) {
        List<String> ret = new LinkedList<String>();

        if (options.containsKey("paths")) {
            ObjectMapper objectMapper = new ObjectMapper();
            String pathsStr = options.getOrDefault("paths", "");
            String[] parsedPathsList;
            try {
                parsedPathsList = objectMapper.readValue(pathsStr, String[].class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Could not parse paths", e);
            }
            for (String p: parsedPathsList) {
                ret.add(p);
            }
        }

        if (options.containsKey("path")) {
            ret.add(options.get("path"));
        }
        Collections.sort(ret);
        return ret;
    }

    public static class DataSourceOptionsAdaptor_v30 extends CaseInsensitiveStringMap implements DataSourceOptionsInterface {
        public DataSourceOptionsAdaptor_v30(java.util.Map<String, String> originalMap) {
            super(originalMap);
        }

        @Override
        public String getOrDefault(String key, String defaultValue) {
            return super.getOrDefault(key, defaultValue);
        }
    }

    public Reader createTestReader(DataSourceOptionsAdaptor options, SparkContext context, boolean traceIO) {
        List<String> paths = getPathsPriv(options);
        return new Reader(paths, options, context, null);
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        DataSourceOptionsAdaptor optionsUpcast = new DataSourceOptionsAdaptor(options);
        List<String> paths = getPathsPriv(new CaseInsensitiveStringMap(options));
        return Reader.getSchemaFromFiles(paths, optionsUpcast);
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        DataSourceOptionsAdaptor optionsUpcast = new DataSourceOptionsAdaptor(properties);
        List<String> paths = getPathsPriv(new CaseInsensitiveStringMap(properties));
        return new Table_v30(optionsUpcast, paths);
    }

}