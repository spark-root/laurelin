package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.spark_ttree.Partition;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;

public class InputPartition_v30 implements InputPartition {
    static final Logger logger = LogManager.getLogger();
    private static final long serialVersionUID = 42L;
    public Partition partition;

    public InputPartition_v30(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches,
            int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
        partition = new Partition(schema, entryStart, entryEnd, slimBranches, threadCount, profileData, pid);
    }
}