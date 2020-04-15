package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;
import scala.Option;

// import org.apache.spark.sql.execution.datasources.v2.FileTable;
/**
 * A collection of ROOT files represented as a Spark "Table".
 * <p>
 * Instantiated once per "df = context.format('root').read()" call. Repeated ops
 * on the same dataframe will use the same Table object and just produce new
 * scan builders.
 */
public class Table_v30 implements Table, SupportsRead {
    static final Logger logger = LogManager.getLogger();

    public Table_v30(LaurelinDSConfig options, List<String> paths) {
        Option<SparkSession> session = SparkSession.getActiveSession();
        this.originalOptions = options;
        this.paths = paths;
        // TODO session can be null
        reader = new Reader(paths, options, session.get().sparkContext());
    }

    private Reader reader;
    private LaurelinDSConfig originalOptions;
    private List<String> paths;

    @Override
    public String name() {
        return "Laurelin_v30_table";
    }

    @Override
    public StructType schema() {
        return reader.readSchema();
    }

    private static final Set<TableCapability> CAPABILITIES = new HashSet<>(Arrays.asList(
            TableCapability.BATCH_READ));

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        if (options != null) {
            LaurelinDSConfig optionsUpcast = LaurelinDSConfig.wrap(options.asCaseSensitiveMap());
            assert optionsUpcast.getMap().equals(originalOptions.getMap());
        }
        return new ScanBuilder_v30(reader);
    }
}