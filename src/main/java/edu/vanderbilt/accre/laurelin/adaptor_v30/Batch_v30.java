package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.Batch;

import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.spark_ttree.Partition;
import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;

public class Batch_v30 implements Batch {
    static final Logger logger = LogManager.getLogger();
    LaurelinDSConfig config;

    private Reader reader;
    public Batch_v30(Reader reader) {
        this.reader = reader;
        config = reader.getConfig();
    }

    @Override
    public InputPartition_v30[] planInputPartitions() {
        List<Partition> internalPartitions = reader.planBatchInputPartitions();
        InputPartition_v30 [] ret = new InputPartition_v30[internalPartitions.size()];
        int idx = 0;
        for (Partition i: internalPartitions) {
            InputPartition_v30 externalPartition = new InputPartition_v30(i.schema,
                                                                            i.entryStart,
                                                                            i.entryEnd,
                                                                            i.slimBranches,
                                                                            config,
                                                                            i.profileData,
                                                                            i.pid);
            ret[idx] = externalPartition;
            idx += 1;
        }

        return ret;
    }

    @Override
    public PartitionReaderFactory_v30 createReaderFactory() {
        return new PartitionReaderFactory_v30();
    }

}
