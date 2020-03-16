package edu.vanderbilt.accre.laurelin.adaptor_v30;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.spark_ttree.Partition;
import edu.vanderbilt.accre.laurelin.spark_ttree.PartitionReader;

public class PartitionReader_v30<T> implements org.apache.spark.sql.connector.read.PartitionReader<T> {
    // The only specialization from spark is ColumnarBatch
    static final Logger logger = LogManager.getLogger();

    private edu.vanderbilt.accre.laurelin.spark_ttree.PartitionReader partitionReader;

    public PartitionReader_v30(InputPartition_v30 partitionWrap) {
        Partition partition = partitionWrap.partition;
        partitionReader = new PartitionReader(partition.schema,
                                                partition.entryStart,
                                                partition.entryEnd,
                                                partition.slimBranches,
                                                partition.threadCount,
                                                partition.profileData,
                                                partition.pid);
    }

    @Override
    public void close() throws IOException {
        partitionReader.close();
    }

    @Override
    public boolean next() throws IOException {
        return partitionReader.next();
    }

    @Override
    public T get() {
        return (T) partitionReader.get();
    }

}