package edu.vanderbilt.accre.laurelin.adaptor_v24;

import java.io.Serializable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.spark_ttree.Partition;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;

/**
 * Represents a Partition of a TTree.
 *
 * <p>This is instantiated on the driver, then serialized and transmitted to
 * the executor
 */
class Partition_v24 implements InputPartition<ColumnarBatch>, Serializable {
    static final Logger logger = LogManager.getLogger();
    private static final long serialVersionUID = 42L;
    private Partition partition;

    public Partition_v24(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
        partition = new Partition(schema, entryStart, entryEnd, slimBranches, threadCount, profileData, pid);
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {
        logger.trace("input partition reader_v24");
        return new PartitionReader_v24(partition.schema,
                                        partition.entryStart,
                                        partition.entryEnd,
                                        partition.slimBranches,
                                        partition.threadCount,
                                        partition.profileData,
                                        partition.pid);
    }

    public void setPid(int pid) {
        partition.setPid(pid);
    }
}