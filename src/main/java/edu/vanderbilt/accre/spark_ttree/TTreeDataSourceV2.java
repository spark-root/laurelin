package edu.vanderbilt.accre.spark_ttree;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupportWithSchema;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsReportPartitioning;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * @author meloam
 *
 */
public class TTreeDataSourceV2 implements DataSourceV2, ReadSupport {
	public class TTreeDataSourceV2Reader implements DataSourceReader, 
													SupportsScanColumnarBatch,
													SupportsPushDownRequiredColumns,
													SupportsReportPartitioning {

		@Override
		public StructType readSchema() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<DataReaderFactory<ColumnarBatch>> createBatchDataReaderFactories() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void pruneColumns(StructType requiredSchema) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public List<DataReaderFactory<Row>> createDataReaderFactories() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Partitioning outputPartitioning() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	@Override
	public DataSourceReader createReader(DataSourceOptions options) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
