package edu.vanderbilt.accre.laurelin.adaptor_v30;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;

public class Scan_v30 implements Scan {
    static final Logger logger = LogManager.getLogger();

    private Reader reader;
    public Scan_v30(Reader reader) {
        this.reader = reader;
    }

    @Override
    public StructType readSchema() {
        return reader.readSchema();
    }

    @Override
    public Batch_v30 toBatch() {
        return new Batch_v30(reader);
    }
}