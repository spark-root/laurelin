package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFileCache;

public class PartitionReader {
    static final Logger logger = LogManager.getLogger();

    private BasketCache basketCache;
    private StructType schema;
    private long entryStart;
    private long entryEnd;
    private int currBasket = -1;
    private Map<String, SlimTBranch> slimBranches;

    /**
     * ThreadPool handling the async decompression tasks
     */
    private static ThreadPoolExecutor staticExecutor;

    /**
     *  (very) surprisingly, a static thread pool executor will prevent the
     *  JVM from ever properly shutting down, because of a circular nature
     *  of the references. Static variables and thread objects are GC roots,
     *  and the thread pool object references its child threads while the
     *  threads themselves reference the threadpool. No amount of GC runs
     *  will make anything unreachable, so the JVM can't finalize and
     *  shut down. To break the cycle, we need to forcibly shutdown the
     *  thread pool, which causes it to kill its threads and allows the GC
     *  to unravel the references.
     *  <p>
     *  see also: https://stackoverflow.com/a/10395700
     */
    static {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("laurelin-arraybuilder-%d").build();
        staticExecutor = new ThreadPoolExecutor(1, 1,
                                                5L, TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                factory);
        staticExecutor.allowCoreThreadTimeOut(true);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                PartitionReader.staticExecutor.shutdownNow();
            }
        });
    }

    /**
     * Holds the async threadpool if enabled, null otherwise
     */
    private static ThreadPoolExecutor executor;
    private CollectionAccumulator<Storage> profileData;
    private int pid;
    private static ROOTFileCache fileCache = ROOTFileCache.getCache();

    private ColumnVector[] currVecs;

    private ColumnVector[] tmp2s;

    public PartitionReader(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, LaurelinDSConfig options, CollectionAccumulator<Storage> profileData, int pid) {
        this.basketCache = BasketCache.getCache();
        this.schema = schema;
        this.entryStart = entryStart;
        this.entryEnd = entryEnd;
        this.slimBranches = slimBranches;
        this.profileData = profileData;
        this.pid = pid;

        Function<Event, Integer> cb = null;
        if (this.profileData != null) {
            cb = e -> {
                this.profileData.add(e.getStorage());
                return 0;
            };
        }
        IOProfile.getInstance(pid, cb);

        int threadCount = options.getInt("threadCount");
        if (threadCount >= 1) {
            executor = staticExecutor;
            executor.setCorePoolSize(threadCount);
            executor.setMaximumPoolSize(threadCount);
        } else {
            executor = null;
        }
    }

    public void close() throws IOException {
        logger.trace("close");
        if (currVecs != null) {
            for (ColumnVector vec: tmp2s) {
                vec.close();
            }
            for (ColumnVector vec: currVecs) {
                vec.close();
            }
        }
        // This will eventually go away due to GC, should I add
        // explicit closing too?
    }

    public boolean next() throws IOException {
        logger.trace("next");
        if (currBasket == -1) {
            // nothing read yet
            currBasket = 0;
            return true;
        } else {
            // we already read the partition
            return false;
        }
    }

    public ColumnarBatch get() {
        logger.trace("columnarbatch");
        LinkedList<ColumnVector> vecs = new LinkedList<ColumnVector>();
        vecs = getBatchRecursive(schema.fields());
        currVecs = new ColumnVector[vecs.size()];
        int idx = 0;
        for (ColumnVector vec: vecs) {
            // Finalize the vectors before we pass it to Spark
            if (vec.getClass() == TTreeColumnVector.class) {
                ((TTreeColumnVector) vec).ensureLoaded();
            }
            currVecs[idx] = vec;
            idx += 1;
        }

        tmp2s = new ColumnVector[vecs.size()];
        idx = 0;
        for (ColumnVector vec: currVecs) {
            //System.out.println("index is " + idx);
            tmp2s[idx] = vec;
            idx += 1;
        }
        // End misery
        ColumnarBatch ret = new ColumnarBatch(tmp2s);
        ret.setNumRows((int) (entryEnd - entryStart));

        return ret;
    }

    private LinkedList<ColumnVector> getBatchRecursive(StructField[] structFields) {
        LinkedList<ColumnVector> vecs = new LinkedList<ColumnVector>();
        for (StructField field: structFields)  {
            if (field.dataType() instanceof StructType) {
                LinkedList<ColumnVector> nestedVecs = getBatchRecursive(((StructType)field.dataType()).fields());
                vecs.add(new StructColumnVector(field.dataType(), nestedVecs));
                continue;
            }
            SlimTBranchInterface slimBranch = slimBranches.get(field.name());
            SimpleType rootType;
            rootType = SimpleType.fromString(field.metadata().getString("rootType"));

            Dtype dtype = SimpleType.dtypeFromString(field.metadata().getString("rootType"));
            vecs.add(TTreeColumnVector.makeColumnVector(field.dataType(), rootType, dtype, basketCache, entryStart, entryEnd, slimBranch, executor, fileCache));
        }
        return vecs;
    }
}