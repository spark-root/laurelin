package edu.vanderbilt.accre.laurelin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.adaptor_v24.Root_v24;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;

public class Root extends Root_v24 {
    static final Logger logger = LogManager.getLogger();

    @Override
    public String shortName() {
        return "root";
    }

    /**
     * Used for unit-tests when there is no current spark context
     * @param options DS options
     * @param context spark context to use
     * @param traceIO whether or not to trace the IO operations
     * @return new reader
     */
    public Reader createTestReader(DataSourceOptions options, SparkContext context, boolean traceIO) {
        logger.trace("Construct new reader");
        CollectionAccumulator<Storage> ioAccum = null;
        if ((traceIO) && (context != null)) {
            synchronized (Root.class) {
                ioAccum = new CollectionAccumulator<Event.Storage>();
                context.register(ioAccum, "edu.vanderbilt.accre.laurelin.ioprofile");
            }
        }
        return new Reader(options, context, ioAccum);
    }
}
