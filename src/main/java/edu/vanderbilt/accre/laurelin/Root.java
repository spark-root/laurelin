package edu.vanderbilt.accre.laurelin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeDataSourceV2Reader;

public class Root implements DataSourceV2, ReadSupport, DataSourceRegister {
    static final Logger logger = LogManager.getLogger();

    

    

    /*
     * This is called by Spark, unlike the following function that accepts a
     * SparkContext
     */

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return createReader(options, SparkContext.getOrCreate());
    }

    /**
     * Used for unit-tests when there is no current spark context
     * @param options DS options
     * @param context spark context to use
     * @return new reader
     */

    public DataSourceReader createReader(DataSourceOptions options, SparkContext context) {
        logger.trace("make new reader");
        CacheFactory basketCacheFactory = new CacheFactory();
        CollectionAccumulator<Event.Storage> ioAccum = null;
        if (context != null) {
            ioAccum = new CollectionAccumulator<Event.Storage>();
            context.register(ioAccum, "edu.vanderbilt.accre.laurelin.ioprofile");
        }
        return new TTreeDataSourceV2Reader(options, basketCacheFactory, context, ioAccum);
    }


    @Override
    public String shortName() {
        return "root";
    }

}