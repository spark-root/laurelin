package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;

/**
 * Represents a Partition of a TTree.
 *
 * <p>This is instantiated on the driver, then serialized and transmitted to
 * the executor
 */
class Partition implements InputPartition<ColumnarBatch> {
    static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = -6598704946339913432L;
    private StructType schema;
    private long entryStart;
    private long entryEnd;
    private Map<String, SlimTBranch> slimBranches;
    private int threadCount;
    private CollectionAccumulator<Storage> profileData;
    private int pid;

    public Partition(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
        logger.trace("dsv2partition new");
        this.schema = schema;
        this.entryStart = entryStart;
        this.entryEnd = entryEnd;
        this.slimBranches = slimBranches;
        this.threadCount = threadCount;
        this.profileData = profileData;
        this.pid = pid;
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {
        logger.trace("input partition reader");
        return new PartitionReader(schema, entryStart, entryEnd, slimBranches, threadCount, profileData, pid);
    }

    public void setPid(int pid) {
        this.pid = pid;
    }
}