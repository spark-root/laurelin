package edu.vanderbilt.accre.laurelin.adaptor_v30;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;

public class ScanBuilder_v30 implements ScanBuilder, SupportsPushDownRequiredColumns {
    static final Logger logger = LogManager.getLogger();

    private Reader reader;
    public ScanBuilder_v30(Reader reader) {
        this.reader = reader;
    }

    @Override
    public Scan_v30 build() {
        return new Scan_v30(reader);
    }

    @Override
    public void pruneColumns(StructType schema) {
        reader.pruneColumns(schema);
    }

}