package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.Serializable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;

/**
 * Represents a Partition of a TTree.
 *
 * <p> Acts like Spark2.4's InputPartition&lt;ColumnarBatch&gt;
 *
 * <p>This is instantiated on the driver, then serialized and transmitted to
 * the executor
 */
public class Partition implements Serializable {
    static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = -6598704946339913432L;
    public StructType schema;
    public long entryStart;
    public long entryEnd;
    public Map<String, SlimTBranch> slimBranches;
    public CollectionAccumulator<Storage> profileData;
    public int pid;
    private LaurelinDSConfig options;

    public Partition(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, LaurelinDSConfig options, CollectionAccumulator<Storage> profileData, int pid) {
        logger.trace("dsv2partition new");
        this.schema = schema;
        this.entryStart = entryStart;
        this.entryEnd = entryEnd;
        this.slimBranches = slimBranches;
        this.options = options;
        this.profileData = profileData;
        this.pid = pid;
    }

    public PartitionReader createPartitionReader() {
        logger.trace("input partition reader");
        return new PartitionReader(schema, entryStart, entryEnd, slimBranches, options, profileData, pid);
    }

    public void setPid(int pid) {
        this.pid = pid;
    }
}
