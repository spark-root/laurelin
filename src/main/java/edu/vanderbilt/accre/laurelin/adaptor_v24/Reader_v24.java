package edu.vanderbilt.accre.laurelin.adaptor_v24;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;

import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.spark_ttree.Partition;
import edu.vanderbilt.accre.laurelin.spark_ttree.Reader;

public class Reader_v24 implements DataSourceReader,
        SupportsScanColumnarBatch,
        SupportsPushDownRequiredColumns {
    private Reader reader;

    public Reader_v24(DataSourceOptions options, SparkContext sparkContext, CollectionAccumulator<Storage> ioAccum) {
        reader = new Reader(options, sparkContext, ioAccum);
    }

    @Override
    public StructType readSchema() {
        return reader.readSchema();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        reader.pruneColumns(requiredSchema);

    }

    @Override
    public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
        List<Partition> internalPartitions = reader.planBatchInputPartitions();
        List<InputPartition<ColumnarBatch>> ret = new ArrayList<InputPartition<ColumnarBatch>>(internalPartitions.size());
        for (Partition i: internalPartitions) {
            Partition_v24 externalPartition = new Partition_v24(i.schema,
                                                                i.entryStart,
                                                                i.entryEnd,
                                                                i.slimBranches,
                                                                i.threadCount,
                                                                i.profileData,
                                                                i.pid);
            ret.add(externalPartition);
        }
        return ret;
    }
}