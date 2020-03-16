package edu.vanderbilt.accre.laurelin.adaptor_v30;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class PartitionReaderFactory_v30 implements PartitionReaderFactory {
    private static final long serialVersionUID = 42L;
    static final Logger logger = LogManager.getLogger();

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        throw new UnsupportedOperationException("Cannot create row-based reader.");
    }

    @Override
    public PartitionReader_v30<ColumnarBatch> createColumnarReader(InputPartition partition) {
        return new PartitionReader_v30<ColumnarBatch>((InputPartition_v30) partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}