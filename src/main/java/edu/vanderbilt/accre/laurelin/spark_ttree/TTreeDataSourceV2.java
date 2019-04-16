package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TLeaf;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;

public class TTreeDataSourceV2 implements DataSourceV2, ReadSupport {
	// InputPartition is serialized and sent to the executor who then makes the
	// InputPartitionReader, I don't think we need to have that division
	public class TTreeDataSourceV2Partition implements InputPartition<ColumnarBatch>,
														InputPartitionReader<ColumnarBatch>{

		private static final long serialVersionUID = -6598704946339913432L;
		private String path;
		private String treeName;
		private StructType schema;
		private TFile file;
		private TTree tree;
		
		public TTreeDataSourceV2Partition(String path, String treeName, StructType schema) {
			this.path = path;
			this.treeName = treeName;
			this.schema = schema;
		}
		
		/*
		 * Begin InputPartition overrides
		 */
		
		@Override
		public InputPartitionReader<ColumnarBatch> createPartitionReader() {
			try {
				file = TFile.getFromFile(path);
				tree = new TTree(file.get(treeName));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return this;
		}
		
		/*
		 * Begin InputPartitionReader overrides
		 */

		@Override
		public void close() throws IOException {
			// This will eventually go away due to GC, should I add
			// explicit closing too?
		}

		@Override
		public boolean next() throws IOException {
			// TODO Auto-generated method stub
			// Need to count the baskets, etc...
			return false;
		}

		@Override
		public ColumnarBatch get() {
			ColumnVector[] vecs = new ColumnVector[schema.fields().length];
			int idx = 0;
			for (StructField field: schema.fields())  {
				if (field.dataType() instanceof StructType) {
					throw new RuntimeException("Nested fields are not supported: " + field.name());
				}
				vecs[idx] = new TTreeColumnVector(field.dataType(), null);
				idx += 1;
			}
			ColumnarBatch ret = new ColumnarBatch(vecs);
			return ret;
		}
	}
	
	public class TTreeDataSourceV2Reader implements DataSourceReader, 
													SupportsScanColumnarBatch {
		private String[] paths;
		private TTree currTree;
		private TFile currFile;
		public TTreeDataSourceV2Reader(DataSourceOptions options) {
			try {
				this.paths = options.paths();
				// FIXME - More than one file, please
				currFile = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
				currTree = new TTree(currFile.get("tree"));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public StructType readSchema() {
			List<TBranch> branches = currTree.getBranches();
			List<StructField> fields = readSchemaPart(branches, "");
			StructField[] fieldArray = new StructField[fields.size()];
			fieldArray = (StructField[]) fields.toArray(fieldArray);
			StructType schema = new StructType(fieldArray);
			return schema;
		}
		
		private List<StructField> readSchemaPart(List<TBranch> branches, String prefix) {
			List<StructField> fields = new ArrayList<StructField>();
			for (TBranch branch: branches ) {
				if (branch.getBranches().size() != 0) {
					List<StructField> subFields = readSchemaPart(branch.getBranches(), prefix);
					StructField[] subFieldArray = new StructField[subFields.size()];
					subFieldArray = (StructField[]) subFields.toArray(subFieldArray);
					StructType subStruct = new StructType(subFieldArray);
					fields.add(new StructField(branch.getName(), subStruct, false, null));
				}
				for (TLeaf leaf: branch.getLeaves()) {
					DataType sparkType = rootToSparkType(leaf.getSimpleType());
					fields.add(new StructField(leaf.getName(), sparkType, false, null));
				}
			}
			return fields;
		}
		
		private DataType rootToSparkType(SimpleType simpleType) {
			DataType ret = null;
			if (simpleType instanceof SimpleType.ScalarType) {
				if (simpleType == SimpleType.Bool) {
					ret = DataTypes.BooleanType;
				} else if (simpleType == SimpleType.Int8) {
					ret = DataTypes.ByteType;
				} else if (simpleType == SimpleType.Int16) {
					ret = DataTypes.ShortType;
				} else if (simpleType == SimpleType.Int32) {
					ret = DataTypes.IntegerType;
				} else if (simpleType == SimpleType.Int64) {
					ret = DataTypes.LongType;
				} else if (simpleType == SimpleType.Float32) {
					ret = DataTypes.FloatType;
				} else if (simpleType == SimpleType.Float64) {
					ret = DataTypes.DoubleType;
				} else if (simpleType == SimpleType.Pointer) {
					ret = DataTypes.LongType;
				}
			} else if (simpleType instanceof SimpleType.ArrayType) {
				SimpleType nested = ((SimpleType.ArrayType) simpleType).getChildType();
				ret = DataTypes.createArrayType(rootToSparkType(nested));
			}
			if (ret == null) {
				throw new RuntimeException("Unable to convert ROOT type '" + simpleType + "' to Spark");
			}
			return ret;
		}

		@Override
		public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
			List<InputPartition<ColumnarBatch>> ret = new ArrayList<InputPartition<ColumnarBatch>>();
			for (String path: paths) {
				// TODO Add an option to pass through a tree name
				ret.add(new TTreeDataSourceV2Partition(path, "tree", readSchema()));
			}
			return ret;
		}
	}

	@Override
	public DataSourceReader createReader(DataSourceOptions options) {
		return new TTreeDataSourceV2Reader(options);
	}
	
}
