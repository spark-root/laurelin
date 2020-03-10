package edu.vanderbilt.accre.laurelin.adaptor_v24;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.spark_ttree.PartitionReader;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;

class PartitionReader_v24 implements InputPartitionReader<ColumnarBatch> {
    static final Logger logger = LogManager.getLogger();

    private PartitionReader partitionReader;

    public PartitionReader_v24(StructType schema, long entryStart, long entryEnd, Map<String, SlimTBranch> slimBranches, int threadCount, CollectionAccumulator<Storage> profileData, int pid) {
        partitionReader = new PartitionReader(schema, entryStart, entryEnd, slimBranches, threadCount, profileData, pid);
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
    public ColumnarBatch get() {
        return partitionReader.get();
    }
}