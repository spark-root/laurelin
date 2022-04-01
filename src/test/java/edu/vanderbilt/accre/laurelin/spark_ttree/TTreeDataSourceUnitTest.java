package edu.vanderbilt.accre.laurelin.spark_ttree;

import static edu.vanderbilt.accre.laurelin.Helpers.getBigTestDataIfExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.CollectionAccumulator;
import org.junit.Test;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import edu.vanderbilt.accre.laurelin.Root;
import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.configuration.LaurelinDSConfig;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch.CompressedBasketInfo;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.io.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.serialization.Proxy;
import scala.reflect.ClassTag;

public class TTreeDataSourceUnitTest {
    private static final Logger logger = LogManager.getLogger();

    /*
     * @lgray reported that too much data was being deserialised when TTrees
     * were loaded and partitions made. Make sure that the bytes read don't
     * balloon accidentally.
     */
    @Test
    public void loadMinimalBytes() throws IOException {
        LinkedList<Storage> accum = new LinkedList<Storage>();
        Function<Event, Integer> cb = e -> {
            accum.add(e.getStorage());
            return 0;
        };
        IOProfile.getInstance().setCB(cb);

        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-foriter.root");
        optmap.put("tree",  "foriter");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);

        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1, schemaCast.size());
        List<Partition> partitions = reader.planBatchInputPartitions();

        // Count all bytes read
        long sumReads = 0;
        long uniqReads = 0;
        RangeSet<Long> readRanges = TreeRangeSet.create();
        for (Storage s: accum) {
            sumReads += s.len;
            Range<Long> r = Range.closedOpen(s.offset, s.offset + s.len);
            readRanges.add(r);
        }

        // Count unique bytes read
        for (Range<Long> r: readRanges.asDescendingSetOfRanges()) {
            uniqReads += r.upperEndpoint() - r.lowerEndpoint();
        }

        // Cap the number of bytes we read to load a file and make partitions
        assertTrue(22000 >= sumReads);
        // ... and the number of unique bytes we read to do the same
        assertTrue(5964 >= uniqReads);
    }

    /*
     * @lgray reported that too much data was being transmitted in partitions.
     * this ended up being because an unnecessary hard reference was being held
     * to the whole tbranch
     */
    @Test
    public void checkSerializedPartitionSize() throws IOException {
        String testPath = getBigTestDataIfExists("testdata/nano_19.root");
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        optmap.put("tree",  "Events");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        List<Partition> partitions = reader.planBatchInputPartitions();

        Partition partition = partitions.get(0);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteArrayInputStream bis;
        ObjectOutput out = null;
        byte[] yourBytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(partition);
            out.flush();
            yourBytes = bos.toByteArray();
            /*
             *  This patch produces 349972 bytes serialized, ensure it doesn't
             *  grow accidentally
             */
            assertTrue("Partition size too large: " + yourBytes.length, yourBytes.length < 352000);

            //System.out.println("Got length: " + yourBytes.length);
            bis = new ByteArrayInputStream(yourBytes);
            ObjectInput in = new ObjectInputStream(bis);
            @SuppressWarnings("unchecked")
            Partition partitionBack = (Partition) in.readObject();
            //System.out.println("got partition" + partitionBack);

        } catch (ClassNotFoundException e) {

        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    private static int getSerializedSize(Object x) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] yourBytes = new byte[0];
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(x);
            out.flush();
            yourBytes = bos.toByteArray();
        } catch (IOException e) {
            // who cares
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return yourBytes.length;
    }

    @Test
    public void testMultipleBasketsForiter() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-foriter.root");
        optmap.put("tree",  "foriter");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        DataType schema = reader.readSchema();
        StructType schemaCast = (StructType) schema;
        assertEquals(1, schemaCast.size());
        List<Partition> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        Partition partition;
        long []expectedCounts = {46};
        for (int i = 0; i < 1; i += 1) {
            partition = partitions.get(i);
            PartitionReader partitionReader = partition.createPartitionReader();
            assertTrue(partitionReader.next());
            ColumnarBatch batch = partitionReader.get();
            assertFalse(partitionReader.next());
            assertEquals(expectedCounts[i], batch.numRows());
        }
    }

    @Test
    public void testLoadingCorruptRoot() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/issue96.root");
        optmap.put("tree",  "tpTree/fitter_tree");
        optmap.put("threadCount", "0");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        // only get a scalar float_t for now since that's all that works
        MetadataBuilder metadata = new MetadataBuilder();
        metadata.putString("rootType", "float");
        StructType prune = new StructType()
                            .add(new StructField("eta", DataTypes.FloatType, false, metadata.build()));
        reader.pruneColumns(prune);
        List<Partition> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        Partition partition;
        long []expectedCounts = {132056};
        for (int i = 0; i < 1; i += 1) {
            //System.out.println("Now reading partition " + i);
            partition = partitions.get(i);
            PartitionReader partitionReader = partition.createPartitionReader();
            assertTrue(partitionReader.next());
            ColumnarBatch batch = partitionReader.get();
            ColumnVector x = batch.column(0);
            x.getFloat(batch.numRows() - 1);
            assertFalse(partitionReader.next());
            assertEquals(expectedCounts[i], batch.numRows());

        }
    }

    @Test
    public void testMultipleBasketsForBigNano() throws IOException {
        String testPath = getBigTestDataIfExists("testdata/A2C66680-E3AA-E811-A854-1CC1DE192766.root");
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", testPath);
        optmap.put("tree",  "Events");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        // only get a scalar float_t for now since that's all that works
        MetadataBuilder metadata = new MetadataBuilder();
        metadata.putString("rootType", "float");
        StructType prune = new StructType()
                            .add(new StructField("CaloMET_pt", DataTypes.FloatType, false, metadata.build()));
        reader.pruneColumns(prune);
        List<Partition> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        Partition partition;
        long []expectedCounts = {161536};
        for (int i = 0; i < 1; i += 1) {
            partition = partitions.get(i);
            PartitionReader partitionReader = partition.createPartitionReader();
            assertTrue(partitionReader.next());
            ColumnarBatch batch = partitionReader.get();
            assertFalse(partitionReader.next());
            assertEquals(expectedCounts[i], batch.numRows());
        }
    }



    //@Test(expected = IllegalStateException.class)
    @Test
    public void testPlanInputPartitions() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        List<Partition> batch = reader.planBatchInputPartitions();
        assertNotNull(batch);
    }

    @Test
    public void testplanBatchInputPartitions() {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        assertNotNull(reader.planBatchInputPartitions());
    }

    @Test
    public void testLoadVectorColumns() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/stdvector.root");
        optmap.put("tree",  "tvec");
        optmap.put("threadCount", "0");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        List<Partition> partitionPlan = reader.planBatchInputPartitions();
        assertNotNull(partitionPlan);
        StructType schema = reader.readSchema();
        //System.out.println(schema.prettyJson());

        Partition partition = partitionPlan.get(0);
        PartitionReader partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        ColumnVector col = batch.column(0);
        ColumnarArray arr = col.getArray(0);
        arr.getFloat(0);
    }

    @Test
    public void testKryoSerialization() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/stdvector.root");
        optmap.put("tree",  "tvec");
        optmap.put("threadCount", "0");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Reader reader = new Reader(opts.paths(), opts, (SparkContext) null, (CollectionAccumulator<Storage>) null);
        List<Partition> partitionPlan = reader.planBatchInputPartitions();
        assertNotNull(partitionPlan);
        StructType schema = reader.readSchema();
        //System.out.println(schema.prettyJson());

        Partition partition = partitionPlan.get(0);
        KryoSerializer serializer = new KryoSerializer(new SparkConf());
        SerializerInstance serializerInstance = serializer.newInstance();
        ClassTag<Partition> ct = scala.reflect.ClassTag$.MODULE$.apply(this.getClass());
        ByteBuffer serializedPartition = serializerInstance.serialize(partition, ct);
        partition = serializerInstance.deserialize(serializedPartition, ct);

        PartitionReader partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        ColumnVector col = batch.column(0);
        ColumnarArray arr = col.getArray(0);
        arr.getFloat(0);
    }

    /**
     * Ideally implements the same call order as the full-up spark test
     *  [TRACE] 17:24:35.974 e.v.a.l.Root - planbatchinputpartitions
     *  [TRACE] 17:24:35.974 e.v.a.l.Root - readschema
     *  [TRACE] 17:24:35.975 e.v.a.l.Root - dsv2partition new
     *  [TRACE] 17:24:36.093 e.v.a.l.Root - input partition reader
     *  [INFO ] 17:24:36.096 e.v.a.l.r.TTree - Ignoring unparsable/empty branch "Str"
     *  [TRACE] 17:24:36.098 e.v.a.l.Root - next
     *  [TRACE] 17:24:36.098 e.v.a.l.Root - columnarbatch
     *  [TRACE] 17:24:36.107 e.v.a.l.Root - close
     * @throws IOException
     */
    @Test
    public void testLoadScalarFloat32() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        List<Partition> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        StructType schema = reader.readSchema();


        Partition partition = partitions.get(0);
        PartitionReader partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        assertFalse(partitionReader.next());
        // 19 branches in this file
        assertEquals(19, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("Float32").get());
        assertEquals(0.0f, float32col.getFloat(0), 0.001);
        assertEquals(99.0f, float32col.getFloat(99), 0.001);

        assertFloatArrayEquals(new float[] { 0.0f, 1.0f }, float32col.getFloats(0, 2));
        assertFloatArrayEquals(new float[] { 10.0f, 11.0f }, float32col.getFloats(10, 2));
        assertFloatArrayEquals(new float[] { 22.0f, 23.0f, 24.0f }, float32col.getFloats(22, 3));
    }

    private void assertFloatArrayEquals(float exp[], float act[]) {
        assertEquals("Arrays same length", exp.length, act.length);
        for (int i = 0; i < exp.length; i += 1) {
            assertEquals("Array index " + i + " mismatched", exp[i], act[i], 0.001);
        }
    }

    @Test
    public void testLoadFixedArrayFloat32() throws IOException {
        Map<String, String> optmap = new HashMap<String, String>();
        optmap.put("path", "testdata/uproot-small-flat-tree.root");
        optmap.put("tree",  "tree");
        LaurelinDSConfig opts = LaurelinDSConfig.wrap(optmap);
        Root source = new Root();
        Reader reader = source.createTestReader(opts, null, true);
        List<Partition> partitions = reader.planBatchInputPartitions();
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        StructType schema = reader.readSchema();

        Partition partition = partitions.get(0);
        PartitionReader partitionReader = partition.createPartitionReader();
        assertTrue(partitionReader.next());
        ColumnarBatch batch = partitionReader.get();
        assertFalse(partitionReader.next());
        // 19 branches in this file
        assertEquals(19, batch.numCols());
        // 100 events in this file
        assertEquals(100, batch.numRows());

        ColumnVector float32col = batch.column((int)schema.getFieldIndex("ArrayFloat32").get());

        assertFloatArrayEquals(new float[] { 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, float32col.getArray(0).toFloatArray());
        assertFloatArrayEquals(new float[] { 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f, 10.0f}, float32col.getArray(10).toFloatArray());
        assertFloatArrayEquals(new float[] { 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f, 31.0f}, float32col.getArray(31).toFloatArray());
    }

    @Test
    public void testScalarI1() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarI1").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new BooleanType(), SimpleType.fromString("bool"), SimpleType.dtypeFromString("bool"), cache, 0, 9, slim, null);
        assertEquals(result.getBoolean(0), false);
        assertEquals(result.getBoolean(1), true);
        assertEquals(result.getBoolean(2), false);
        assertEquals(result.getBoolean(3), true);
        assertEquals(result.getBoolean(4), false);
        assertEquals(result.getBoolean(5), true);
        assertEquals(result.getBoolean(6), false);
        assertEquals(result.getBoolean(7), true);
        assertEquals(result.getBoolean(8), false);
    }

    @Test
    public void testArrayI1() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayI1").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new BooleanType(), false), new SimpleType.ArrayType(SimpleType.fromString("bool")), SimpleType.dtypeFromString("bool"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getBoolean(0), false);
        assertEquals(event0.getBoolean(1), false);
        assertEquals(event0.getBoolean(2), false);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getBoolean(0), true);
        assertEquals(event1.getBoolean(1), true);
        assertEquals(event1.getBoolean(2), true);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getBoolean(0), false);
        assertEquals(event2.getBoolean(1), false);
        assertEquals(event2.getBoolean(2), false);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getBoolean(0), true);
        assertEquals(event3.getBoolean(1), true);
        assertEquals(event3.getBoolean(2), true);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getBoolean(0), false);
        assertEquals(event4.getBoolean(1), false);
        assertEquals(event4.getBoolean(2), false);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getBoolean(0), true);
        assertEquals(event5.getBoolean(1), true);
        assertEquals(event5.getBoolean(2), true);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getBoolean(0), false);
        assertEquals(event6.getBoolean(1), false);
        assertEquals(event6.getBoolean(2), false);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getBoolean(0), true);
        assertEquals(event7.getBoolean(1), true);
        assertEquals(event7.getBoolean(2), true);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getBoolean(0), false);
        assertEquals(event8.getBoolean(1), false);
        assertEquals(event8.getBoolean(2), false);
    }

    @Test
    public void testScalarI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ByteType(), SimpleType.fromString("char"), SimpleType.dtypeFromString("char"), cache, 0, 9, slim, null);
        assertEquals(result.getByte(0), 0);
        assertEquals(result.getByte(1), 1);
        assertEquals(result.getByte(2), 2);
        assertEquals(result.getByte(3), -128);
        assertEquals(result.getByte(4), -127);
        assertEquals(result.getByte(5), -126);
        assertEquals(result.getByte(6), 127);
        assertEquals(result.getByte(7), 126);
        assertEquals(result.getByte(8), 125);
    }

    @Test
    public void testScalarUI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarUI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ShortType(), SimpleType.fromString("uchar"), SimpleType.dtypeFromString("uchar"), cache, 0, 9, slim, null);
        assertEquals(result.getShort(0), 0);
        assertEquals(result.getShort(1), 1);
        assertEquals(result.getShort(2), 2);
        assertEquals(result.getShort(3), 0);
        assertEquals(result.getShort(4), 1);
        assertEquals(result.getShort(5), 2);
        assertEquals(result.getShort(6), 255);
        assertEquals(result.getShort(7), 254);
        assertEquals(result.getShort(8), 253);
    }

    @Test
    public void testArrayI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new ByteType(), false), new SimpleType.ArrayType(SimpleType.fromString("char")), SimpleType.dtypeFromString("char"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getByte(0), 0);
        assertEquals(event0.getByte(1), 0);
        assertEquals(event0.getByte(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getByte(0), 1);
        assertEquals(event1.getByte(1), 1);
        assertEquals(event1.getByte(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getByte(0), 2);
        assertEquals(event2.getByte(1), 2);
        assertEquals(event2.getByte(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getByte(0), -128);
        assertEquals(event3.getByte(1), -128);
        assertEquals(event3.getByte(2), -128);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getByte(0), -127);
        assertEquals(event4.getByte(1), -127);
        assertEquals(event4.getByte(2), -127);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getByte(0), -126);
        assertEquals(event5.getByte(1), -126);
        assertEquals(event5.getByte(2), -126);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getByte(0), 127);
        assertEquals(event6.getByte(1), 127);
        assertEquals(event6.getByte(2), 127);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getByte(0), 126);
        assertEquals(event7.getByte(1), 126);
        assertEquals(event7.getByte(2), 126);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getByte(0), 125);
        assertEquals(event8.getByte(1), 125);
        assertEquals(event8.getByte(2), 125);
    }

    @Test
    public void testArrayUI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayUI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new ShortType(), false), new SimpleType.ArrayType(SimpleType.fromString("uchar")), SimpleType.dtypeFromString("uchar"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getShort(0), 0);
        assertEquals(event0.getShort(1), 0);
        assertEquals(event0.getShort(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getShort(0), 1);
        assertEquals(event1.getShort(1), 1);
        assertEquals(event1.getShort(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getShort(0), 2);
        assertEquals(event2.getShort(1), 2);
        assertEquals(event2.getShort(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getShort(0), 0);
        assertEquals(event3.getShort(1), 0);
        assertEquals(event3.getShort(2), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getShort(0), 1);
        assertEquals(event4.getShort(1), 1);
        assertEquals(event4.getShort(2), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getShort(0), 2);
        assertEquals(event5.getShort(1), 2);
        assertEquals(event5.getShort(2), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getShort(0), 255);
        assertEquals(event6.getShort(1), 255);
        assertEquals(event6.getShort(2), 255);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getShort(0), 254);
        assertEquals(event7.getShort(1), 254);
        assertEquals(event7.getShort(2), 254);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getShort(0), 253);
        assertEquals(event8.getShort(1), 253);
        assertEquals(event8.getShort(2), 253);
    }

    @Test
    public void testSliceI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new ByteType(), false), new SimpleType.ArrayType(SimpleType.fromString("char")), SimpleType.dtypeFromString("char"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getByte(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getByte(0), 2);
        assertEquals(event2.getByte(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getByte(0), -127);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getByte(0), -126);
        assertEquals(event5.getByte(1), -126);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getByte(0), 126);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getByte(0), 125);
        assertEquals(event8.getByte(1), 125);
    }

    @Test
    public void testSliceUI8() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceUI8").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector tmp = new TTreeColumnVector(new ArrayType(new ShortType(), false), new SimpleType.ArrayType(SimpleType.fromString("uchar")), SimpleType.dtypeFromString("uchar"), cache, 0, 9, slim, null);
        ArrowColumnVector result = tmp.toArrowVector();
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getShort(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getShort(0), 2);
        assertEquals(event2.getShort(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getShort(0), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getShort(0), 2);
        assertEquals(event5.getShort(1), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getShort(0), 254);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getShort(0), 253);
        assertEquals(event8.getShort(1), 253);
        result.close();
        tmp.close();
    }

    @Test
    public void testTmp() {
//        [TRACE] 18:40:54.973 e.v.a.l.r.TBranch - 9 entries
//        [TRACE] 18:40:54.973 e.v.a.l.r.TBranch - 10 maxbaskets
//        [TRACE] 18:40:54.974 e.v.a.l.r.TBranch - [96, 0, 0, 0, 0, 0, 0, 0, 0, 0] basketbytes
//        [TRACE] 18:40:54.974 e.v.a.l.r.TBranch - [0, 9, 0, 0, 0, 0, 0, 0, 0, 0] basketentry
//        [TRACE] 18:40:54.974 e.v.a.l.r.TBranch - [1558, 0, 0, 0, 0, 0, 0, 0, 0, 0] basketseek
//        [TRACE] 18:40:54.974 e.v.a.l.r.TBranch - 1 writebasket
        long fEntries = 9;
        int fMaxBaskets = 10;
        int[] fBasketBytesTmp = {96, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        long[] fBasketEntryTmp = {0, 9, 0, 0, 0, 0, 0, 0, 0, 0};
        long[] fBasketSeekTmp = {1558, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        Proxy[] fBaskets = {};
        int fWriteBasket = 1;
        TBranch.CompressedBasketInfo[] ret = basketDemo(fEntries, fMaxBaskets, fBasketBytesTmp, fBasketEntryTmp, fBasketSeekTmp, fBaskets, fWriteBasket);
        assertEquals(2, ret.length);
        logger.trace(ret[0]);
    }

    public static class BranchBasketsLocator {
        private int nEntries = 0;
        private int size = 0;
        private boolean[] isLoose;
        /**
         * Number of bytes in this basket
         */
        private int[] bytes;
        /**
         * Number of entries in this basket
         */
        private long[] entries;
        /**
         * Seek position of this basket in the file
         */
        private long[] seek;
        /**
         * Length of the header at the beginning of a compressed range
         */
        private int[] headerLen;
        /**
         * Length of the compressed range
         */
        private int[] compressedLen;
        /**
         * Length of the compressed range after decompression
         */
        private int[] uncompressedLen;
        /**
         * Offset to the first byte of this compressed range
         */
        private long[] compressedParentOffset;

        private void resize(int size) {
            this.size = size;
            isLoose = Arrays.copyOf(isLoose, size);
            bytes = Arrays.copyOf(bytes, size);
            entries = Arrays.copyOf(entries, size);
            seek = Arrays.copyOf(seek, size);
            headerLen = Arrays.copyOf(headerLen, size);
            compressedLen =  Arrays.copyOf(compressedLen, size);
            uncompressedLen = Arrays.copyOf(uncompressedLen, size);
            compressedParentOffset = Arrays.copyOf(compressedParentOffset, size);
        }

        public BranchBasketsLocator(int size) {
            this.size = size;
            isLoose = new boolean[size];
            bytes = new int[size];
            entries = new long[size];
            seek = new long[size];
            headerLen = new int[size];
            compressedLen = new int[size];
            uncompressedLen = new int[size];
            compressedParentOffset = new long[size];
        }
    }

//    public static TBranch.CompressedBasketInfo[] basketDemo2(long fEntries, int fMaxBaskets, int[] fBasketBytesTmp,
//            long[] fBasketEntryTmp, long[] fBasketSeekTmp, Proxy[] fBaskets,
//            int fWriteBasket) {
//        /*
//         *  Root sometimes makes zero-length/empty baskets, so we need to
//         *  trim them to preserve the invariant in ArrayBuilder that the
//         *  values are monotonically increasing
//         */
//        int nonEmptyBaskets = 0;
//
//        for (int i = 0; i < fMaxBaskets; i += 1) {
//            if (fBasketSeekTmp[i] != 0) {
//                nonEmptyBaskets += 1;
//            }
//        }
//        fBasketBytes = new int[nonEmptyBaskets];
//        fBasketEntry = new long[nonEmptyBaskets];
//        fBasketSeek = new long[nonEmptyBaskets];
//        TBranch.CompressedBasketInfo[] compressedBasketInfo = new CompressedBasketInfo[nonEmptyBaskets];
//
//        int j = 0;
//        for (int i = 0; i < nonEmptyBaskets; i += 1) {
//            if (fBasketSeekTmp[i] != 0) {
//                fBasketBytes[j] = fBasketBytesTmp[i];
//                fBasketEntry[j] = fBasketEntryTmp[i];
//                fBasketSeek[j] = fBasketSeekTmp[i];
//                j += 1;
//            }
//        }
//        fMaxBaskets = nonEmptyBaskets;
//    }

    public static TBranch.CompressedBasketInfo[] basketDemo(long fEntries, int fMaxBaskets, int[] fBasketBytesTmp,
                                                            long[] fBasketEntryTmp, long[] fBasketSeekTmp, Proxy[] fBaskets,
                                                            int fWriteBasket) {
        long lastEntry = 0;
        // Count the valid loose baskets
        int looseBaskets = 0;
        for (int i = 0; i < fMaxBaskets; i += 1) {
            if (((fBasketSeekTmp[i] != 0) && (i != fWriteBasket))) {
                // this is a basket worth deserializing
                looseBaskets += 1;
                lastEntry = fBasketEntryTmp[i];
            } else {
                lastEntry = fBasketEntryTmp[i];
            }
        }

        int embeddedBaskets = 0;
        if (fEntries == fBasketEntryTmp[looseBaskets]) {
            /*
             *  All the baskets in the "regular" location add up to the
             *  number of entries the TBranch purports to have, so we know
             *  there are no "embedded" baskets to search for
             */
        } else if ((fBaskets != null) && (fBaskets.length > 0)) {
            embeddedBaskets = fBaskets.length;
        }


        int correctedBasketCount = looseBaskets + embeddedBaskets + 1;
        int[] fBasketBytes = new int[correctedBasketCount];
        long[] fBasketEntry = new long[correctedBasketCount];
        long[] fBasketSeek = new long[correctedBasketCount];
        TBranch.CompressedBasketInfo[] compressedBasketInfo = new CompressedBasketInfo[correctedBasketCount];
        int j = 0;
        // Loose baskets are the easy case, the TBranch gives these vals
        for (int i = 0; i < looseBaskets; i += 1) {
            fBasketBytes[j] = fBasketBytesTmp[i];
            fBasketEntry[j] = fBasketEntryTmp[i];
            fBasketSeek[j] = fBasketSeekTmp[i];
            // Loose basekets aren't stored within another compressed range
            compressedBasketInfo[j] = null;
            logger.trace("Add entry loose " + j + " entry " + fBasketEntry[j]);
            j += 1;
        }

        if (embeddedBaskets > 0) {
            logger.trace(" - Embedded showing up");
            /*
             * In the case of embedded baskets, push forward lastEvent by
             * the number of events in the final loose basket
             */
        }

        /*
         * For embedded baskets, we need to (annoyingly) do some math to
         * calculate the fBasketEntry. Reminder: lastEntry is one past
         * the final valid entry in the loose baskets, so when we add the
         * number of entries in the embedded basket, the new lastEntry is
         * also one past the last entry in the embedded basket.
         */
        for (int i = 0; i < embeddedBaskets; i += 1) {
            Proxy basket = fBaskets[i];
            fBasketBytes[j] = (int) basket.getScalar("fNbytes").getVal();
            // We track where the TKey is read in our custom streamer override
            fBasketSeek[j] = (long) basket.getScalar("laurelinBasketOffset").getVal();
            Long[] tmpInfo = (Long []) basket.getScalar("laurelinBasketCompressedInfo").getVal();
            compressedBasketInfo[j] = null; //new CompressedBasketInfo(tmpInfo[0].intValue(), tmpInfo[1].intValue(), tmpInfo[2].intValue(), tmpInfo[3], 30);
            long entryFromBasket = (int) basket.getScalar("fNevBuf").getVal();
            logger.trace(" - Embedded has " + entryFromBasket + " events");
            /*
             * first set the current basket beginning, then push lastEntry
             * forward past the end of this basket
             */
            fBasketEntry[j] = lastEntry;
            lastEntry += entryFromBasket;
            logger.trace("Add entry embed " + j + " entry " + lastEntry);
            j += 1;
        }

        fMaxBaskets = j;
        /*
         * Finally we add back in the fake basket who is one past the last
         * valid basket.
         */
        fBasketEntry[j] = lastEntry;
        compressedBasketInfo[j] = null;
        //TBranch.CompressedBasketInfo[] ret = { new TBranch.CompressedBasketInfo(1,2,3,4) };
        return compressedBasketInfo;
    }

    @Test
    public void testScalarI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ShortType(), SimpleType.fromString("short"), SimpleType.dtypeFromString("short"), cache, 0, 9, slim, null);
        assertEquals(result.getShort(0), 0);
        assertEquals(result.getShort(1), 1);
        assertEquals(result.getShort(2), 2);
        assertEquals(result.getShort(3), -32768);
        assertEquals(result.getShort(4), -32767);
        assertEquals(result.getShort(5), -32766);
        assertEquals(result.getShort(6), 32767);
        assertEquals(result.getShort(7), 32766);
        assertEquals(result.getShort(8), 32765);
    }

    @Test
    public void testScalarUI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarUI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new IntegerType(), SimpleType.fromString("ushort"), SimpleType.dtypeFromString("ushort"), cache, 0, 9, slim, null);
        assertEquals(result.getInt(0), 0);
        assertEquals(result.getInt(1), 1);
        assertEquals(result.getInt(2), 2);
        assertEquals(result.getInt(3), 0);
        assertEquals(result.getInt(4), 1);
        assertEquals(result.getInt(5), 2);
        assertEquals(result.getInt(6), 65535);
        assertEquals(result.getInt(7), 65534);
        assertEquals(result.getInt(8), 65533);
    }

    @Test
    public void testArrayI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new ShortType(), false), new SimpleType.ArrayType(SimpleType.fromString("short")), SimpleType.dtypeFromString("short"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getShort(0), 0);
        assertEquals(event0.getShort(1), 0);
        assertEquals(event0.getShort(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getShort(0), 1);
        assertEquals(event1.getShort(1), 1);
        assertEquals(event1.getShort(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getShort(0), 2);
        assertEquals(event2.getShort(1), 2);
        assertEquals(event2.getShort(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getShort(0), -32768);
        assertEquals(event3.getShort(1), -32768);
        assertEquals(event3.getShort(2), -32768);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getShort(0), -32767);
        assertEquals(event4.getShort(1), -32767);
        assertEquals(event4.getShort(2), -32767);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getShort(0), -32766);
        assertEquals(event5.getShort(1), -32766);
        assertEquals(event5.getShort(2), -32766);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getShort(0), 32767);
        assertEquals(event6.getShort(1), 32767);
        assertEquals(event6.getShort(2), 32767);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getShort(0), 32766);
        assertEquals(event7.getShort(1), 32766);
        assertEquals(event7.getShort(2), 32766);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getShort(0), 32765);
        assertEquals(event8.getShort(1), 32765);
        assertEquals(event8.getShort(2), 32765);
    }

    @Test
    public void testArrayUI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayUI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new IntegerType(), false), new SimpleType.ArrayType(SimpleType.fromString("ushort")), SimpleType.dtypeFromString("ushort"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getInt(0), 0);
        assertEquals(event0.getInt(1), 0);
        assertEquals(event0.getInt(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getInt(0), 1);
        assertEquals(event1.getInt(1), 1);
        assertEquals(event1.getInt(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getInt(0), 2);
        assertEquals(event2.getInt(1), 2);
        assertEquals(event2.getInt(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getInt(0), 0);
        assertEquals(event3.getInt(1), 0);
        assertEquals(event3.getInt(2), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getInt(0), 1);
        assertEquals(event4.getInt(1), 1);
        assertEquals(event4.getInt(2), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getInt(0), 2);
        assertEquals(event5.getInt(1), 2);
        assertEquals(event5.getInt(2), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getInt(0), 65535);
        assertEquals(event6.getInt(1), 65535);
        assertEquals(event6.getInt(2), 65535);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getInt(0), 65534);
        assertEquals(event7.getInt(1), 65534);
        assertEquals(event7.getInt(2), 65534);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getInt(0), 65533);
        assertEquals(event8.getInt(1), 65533);
        assertEquals(event8.getInt(2), 65533);
    }

    @Test
    public void testSliceI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new ShortType(), false), new SimpleType.ArrayType(SimpleType.fromString("short")), SimpleType.dtypeFromString("short"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getShort(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getShort(0), 2);
        assertEquals(event2.getShort(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getShort(0), -32767);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getShort(0), -32766);
        assertEquals(event5.getShort(1), -32766);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getShort(0), 32766);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getShort(0), 32765);
        assertEquals(event8.getShort(1), 32765);
    }

    @Test
    public void testSliceUI16() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceUI16").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new IntegerType(), false), new SimpleType.ArrayType(SimpleType.fromString("ushort")), SimpleType.dtypeFromString("ushort"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getInt(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getInt(0), 2);
        assertEquals(event2.getInt(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getInt(0), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getInt(0), 2);
        assertEquals(event5.getInt(1), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getInt(0), 65534);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getInt(0), 65533);
        assertEquals(event8.getInt(1), 65533);
    }

    @Test
    public void testScalarI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new IntegerType(), SimpleType.fromString("int"), SimpleType.dtypeFromString("int"), cache, 0, 9, slim, null);
        assertEquals(result.getInt(0), 0);
        assertEquals(result.getInt(1), 1);
        assertEquals(result.getInt(2), 2);
        assertEquals(result.getInt(3), -2147483648);
        assertEquals(result.getInt(4), -2147483647);
        assertEquals(result.getInt(5), -2147483646);
        assertEquals(result.getInt(6), 2147483647);
        assertEquals(result.getInt(7), 2147483646);
        assertEquals(result.getInt(8), 2147483645);
    }

    @Test
    public void testScalarUI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarUI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new LongType(), SimpleType.fromString("uint"), SimpleType.dtypeFromString("uint"), cache, 0, 9, slim, null);
        assertEquals(result.getLong(0), 0);
        assertEquals(result.getLong(1), 1);
        assertEquals(result.getLong(2), 2);
        assertEquals(result.getLong(3), 0);
        assertEquals(result.getLong(4), 1);
        assertEquals(result.getLong(5), 2);
        assertEquals(result.getLong(6), 4294967295L);
        assertEquals(result.getLong(7), 4294967294L);
        assertEquals(result.getLong(8), 4294967293L);
    }

    @Test
    public void testArrayI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new IntegerType(), false), new SimpleType.ArrayType(SimpleType.fromString("int")), SimpleType.dtypeFromString("int"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getInt(0), 0);
        assertEquals(event0.getInt(1), 0);
        assertEquals(event0.getInt(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getInt(0), 1);
        assertEquals(event1.getInt(1), 1);
        assertEquals(event1.getInt(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getInt(0), 2);
        assertEquals(event2.getInt(1), 2);
        assertEquals(event2.getInt(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getInt(0), -2147483648);
        assertEquals(event3.getInt(1), -2147483648);
        assertEquals(event3.getInt(2), -2147483648);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getInt(0), -2147483647);
        assertEquals(event4.getInt(1), -2147483647);
        assertEquals(event4.getInt(2), -2147483647);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getInt(0), -2147483646);
        assertEquals(event5.getInt(1), -2147483646);
        assertEquals(event5.getInt(2), -2147483646);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getInt(0), 2147483647);
        assertEquals(event6.getInt(1), 2147483647);
        assertEquals(event6.getInt(2), 2147483647);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getInt(0), 2147483646);
        assertEquals(event7.getInt(1), 2147483646);
        assertEquals(event7.getInt(2), 2147483646);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getInt(0), 2147483645);
        assertEquals(event8.getInt(1), 2147483645);
        assertEquals(event8.getInt(2), 2147483645);
    }

    @Test
    public void testArrayUI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayUI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new LongType(), false), new SimpleType.ArrayType(SimpleType.fromString("uint")), SimpleType.dtypeFromString("uint"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getLong(0), 0);
        assertEquals(event0.getLong(1), 0);
        assertEquals(event0.getLong(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getLong(0), 1);
        assertEquals(event1.getLong(1), 1);
        assertEquals(event1.getLong(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getLong(0), 2);
        assertEquals(event2.getLong(1), 2);
        assertEquals(event2.getLong(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getLong(0), 0);
        assertEquals(event3.getLong(1), 0);
        assertEquals(event3.getLong(2), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getLong(0), 1);
        assertEquals(event4.getLong(1), 1);
        assertEquals(event4.getLong(2), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getLong(0), 2);
        assertEquals(event5.getLong(1), 2);
        assertEquals(event5.getLong(2), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getLong(0), 4294967295L);
        assertEquals(event6.getLong(1), 4294967295L);
        assertEquals(event6.getLong(2), 4294967295L);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getLong(0), 4294967294L);
        assertEquals(event7.getLong(1), 4294967294L);
        assertEquals(event7.getLong(2), 4294967294L);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getLong(0), 4294967293L);
        assertEquals(event8.getLong(1), 4294967293L);
        assertEquals(event8.getLong(2), 4294967293L);
    }

    @Test
    public void testSliceI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new IntegerType(), false), new SimpleType.ArrayType(SimpleType.fromString("int")), SimpleType.dtypeFromString("int"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getInt(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getInt(0), 2);
        assertEquals(event2.getInt(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getInt(0), -2147483647);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getInt(0), -2147483646);
        assertEquals(event5.getInt(1), -2147483646);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getInt(0), 2147483646);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getInt(0), 2147483645);
        assertEquals(event8.getInt(1), 2147483645);
    }

    @Test
    public void testSliceUI32() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceUI32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new LongType(), false), new SimpleType.ArrayType(SimpleType.fromString("uint")), SimpleType.dtypeFromString("uint"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getLong(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getLong(0), 2);
        assertEquals(event2.getLong(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getLong(0), 1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getLong(0), 2);
        assertEquals(event5.getLong(1), 2);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getLong(0), 4294967294L);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getLong(0), 4294967293L);
        assertEquals(event8.getLong(1), 4294967293L);
    }

    @Test
    public void testScalarI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new LongType(), SimpleType.fromString("long"), SimpleType.dtypeFromString("long"), cache, 0, 9, slim, null);
        assertEquals(result.getLong(0), 0);
        assertEquals(result.getLong(1), 1);
        assertEquals(result.getLong(2), 2);
        assertEquals(result.getLong(3), -9223372036854775808L);
        assertEquals(result.getLong(4), -9223372036854775807L);
        assertEquals(result.getLong(5), -9223372036854775806L);
        assertEquals(result.getLong(6), 9223372036854775807L);
        assertEquals(result.getLong(7), 9223372036854775806L);
        assertEquals(result.getLong(8), 9223372036854775805L);
    }

    @Test
    public void testScalarUI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ScalarUI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new DoubleType(), SimpleType.fromString("ulong"), SimpleType.dtypeFromString("ulong"), cache, 0, 9, slim, null);
        assertEquals(result.getLong(0), 0, 0.1);
        assertEquals(result.getLong(1), 1, 0.1);
        assertEquals(result.getLong(2), 2, 0.1);
        assertEquals(result.getLong(3), 0, 0.1);
        assertEquals(result.getLong(4), 1, 0.1);
        assertEquals(result.getLong(5), 2, 0.1);
        /*
         *  There's not enough bits in the mantissa to be able to tell the
         *  difference between ULONG_MAX and (ULONG_MAX - 1). Oh well.
         */
        assertEquals(result.getLong(6), -1, 0.1);
        assertEquals(result.getLong(7), -2, 0.1);
        assertEquals(result.getLong(8), -3, 0.1);
    }

    @Test
    public void testArrayI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new LongType(), false), new SimpleType.ArrayType(SimpleType.fromString("long")), SimpleType.dtypeFromString("long"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getLong(0), 0);
        assertEquals(event0.getLong(1), 0);
        assertEquals(event0.getLong(2), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getLong(0), 1);
        assertEquals(event1.getLong(1), 1);
        assertEquals(event1.getLong(2), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getLong(0), 2);
        assertEquals(event2.getLong(1), 2);
        assertEquals(event2.getLong(2), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getLong(0), -9223372036854775808L);
        assertEquals(event3.getLong(1), -9223372036854775808L);
        assertEquals(event3.getLong(2), -9223372036854775808L);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getLong(0), -9223372036854775807L);
        assertEquals(event4.getLong(1), -9223372036854775807L);
        assertEquals(event4.getLong(2), -9223372036854775807L);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getLong(0), -9223372036854775806L);
        assertEquals(event5.getLong(1), -9223372036854775806L);
        assertEquals(event5.getLong(2), -9223372036854775806L);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getLong(0), 9223372036854775807L);
        assertEquals(event6.getLong(1), 9223372036854775807L);
        assertEquals(event6.getLong(2), 9223372036854775807L);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getLong(0), 9223372036854775806L);
        assertEquals(event7.getLong(1), 9223372036854775806L);
        assertEquals(event7.getLong(2), 9223372036854775806L);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getLong(0), 9223372036854775805L);
        assertEquals(event8.getLong(1), 9223372036854775805L);
        assertEquals(event8.getLong(2), 9223372036854775805L);
    }

    @Test
    public void testArrayUI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("ArrayUI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new DoubleType(), false), new SimpleType.ArrayType(SimpleType.fromString("ulong")), SimpleType.dtypeFromString("ulong"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 3);
        assertEquals(event0.getLong(0), 0, 0.1);
        assertEquals(event0.getLong(1), 0, 0.1);
        assertEquals(event0.getLong(2), 0, 0.1);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 3);
        assertEquals(event1.getLong(0), 1, 0.1);
        assertEquals(event1.getLong(1), 1, 0.1);
        assertEquals(event1.getLong(2), 1, 0.1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 3);
        assertEquals(event2.getLong(0), 2, 0.1);
        assertEquals(event2.getLong(1), 2, 0.1);
        assertEquals(event2.getLong(2), 2, 0.1);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 3);
        assertEquals(event3.getLong(0), 0, 0.1);
        assertEquals(event3.getLong(1), 0, 0.1);
        assertEquals(event3.getLong(2), 0, 0.1);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 3);
        assertEquals(event4.getLong(0), 1, 0.1);
        assertEquals(event4.getLong(1), 1, 0.1);
        assertEquals(event4.getLong(2), 1, 0.1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 3);
        assertEquals(event5.getLong(0), 2, 0.1);
        assertEquals(event5.getLong(1), 2, 0.1);
        assertEquals(event5.getLong(2), 2, 0.1);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 3);
        assertEquals(event6.getLong(0), -1, 0.1);
        assertEquals(event6.getLong(1), -1, 0.1);
        assertEquals(event6.getLong(2), -1, 0.1);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 3);
        assertEquals(event7.getLong(0), -2, 0.1);
        assertEquals(event7.getLong(1), -2, 0.1);
        assertEquals(event7.getLong(2), -2, 0.1);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 3);
        assertEquals(event8.getLong(0), -3, 0.1);
        assertEquals(event8.getLong(1), -3, 0.1);
        assertEquals(event8.getLong(2), -3, 0.1);
    }

    @Test
    public void testSliceI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new LongType(), false), new SimpleType.ArrayType(SimpleType.fromString("long")), SimpleType.dtypeFromString("long"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getLong(0), 1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getLong(0), 2);
        assertEquals(event2.getLong(1), 2);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getLong(0), -9223372036854775807L);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getLong(0), -9223372036854775806L);
        assertEquals(event5.getLong(1), -9223372036854775806L);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getLong(0), 9223372036854775806L);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getLong(0), 9223372036854775805L);
        assertEquals(event8.getLong(1), 9223372036854775805L);
    }

    @Test
    public void testSliceUI64() throws IOException {
        TFile file = TFile.getFromFile("testdata/all-types.root");
        TTree tree = new TTree(file.getProxy("Events"), file);
        TBranch branch = tree.getBranches("SliceUI64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new DoubleType(), false), new SimpleType.ArrayType(SimpleType.fromString("ulong")), SimpleType.dtypeFromString("ulong"), cache, 0, 9, slim, null);
        ColumnarArray event0 = result.getArray(0);
        assertEquals(event0.numElements(), 0);
        ColumnarArray event1 = result.getArray(1);
        assertEquals(event1.numElements(), 1);
        assertEquals(event1.getLong(0), 1, 0.1);
        ColumnarArray event2 = result.getArray(2);
        assertEquals(event2.numElements(), 2);
        assertEquals(event2.getLong(0), 2, 0.1);
        assertEquals(event2.getLong(1), 2, 0.1);
        ColumnarArray event3 = result.getArray(3);
        assertEquals(event3.numElements(), 0);
        ColumnarArray event4 = result.getArray(4);
        assertEquals(event4.numElements(), 1);
        assertEquals(event4.getLong(0), 1, 0.1);
        ColumnarArray event5 = result.getArray(5);
        assertEquals(event5.numElements(), 2);
        assertEquals(event5.getLong(0), 2, 0.1);
        assertEquals(event5.getLong(1), 2, 0.1);
        ColumnarArray event6 = result.getArray(6);
        assertEquals(event6.numElements(), 0);
        ColumnarArray event7 = result.getArray(7);
        assertEquals(event7.numElements(), 1);
        assertEquals(event7.getLong(0), -2, 0.1);
        ColumnarArray event8 = result.getArray(8);
        assertEquals(event8.numElements(), 2);
        assertEquals(event8.getLong(0), -3, 0.1);
        assertEquals(event8.getLong(1), -3, 0.1);
        result.close();
    }

    @Test
    public void testFloat32() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("Float32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new FloatType(), SimpleType.fromString("float"), SimpleType.dtypeFromString("float"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            assertEquals(result.getFloat(i), i, 0.0001);
        }
        result.close();
    }

    @Test
    public void testArrayFloat32() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("ArrayFloat32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new FloatType(), false), new SimpleType.ArrayType(SimpleType.fromString("float")), SimpleType.dtypeFromString("float"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            ColumnarArray event = result.getArray(i);
            assertEquals(event.numElements(), 10);
            for (int j = 0;  j < 10;  j++) {
                assertEquals(event.getFloat(j), i, 0.0001);
            }
        }
        result.close();
    }

    @Test
    public void testSliceFloat32() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("SliceFloat32").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new FloatType(), false), new SimpleType.ArrayType(SimpleType.fromString("float")), SimpleType.dtypeFromString("float"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            ColumnarArray event = result.getArray(i);
            assertEquals(event.numElements(), i % 10);
            for (int j = 0;  j < i % 10;  j++) {
                assertEquals(event.getFloat(j), i, 0.0001);
            }
        }
    }

    @Test
    public void testFloat64() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("Float64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new DoubleType(), SimpleType.fromString("double"), SimpleType.dtypeFromString("double"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            assertEquals(result.getDouble(i), i, 0.0001);
        }
        result.close();
    }

    @Test
    public void testArrayFloat64() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("ArrayFloat64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new DoubleType(), false), new SimpleType.ArrayType(SimpleType.fromString("double")), SimpleType.dtypeFromString("double"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            ColumnarArray event = result.getArray(i);
            assertEquals(event.numElements(), 10);
            for (int j = 0;  j < 10;  j++) {
                assertEquals(event.getDouble(j), i, 0.0001);
            }
        }
        result.close();
    }

    @Test
    public void testSliceFloat64() throws IOException {
        TFile file = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        TTree tree = new TTree(file.getProxy("tree"), file);
        TBranch branch = tree.getBranches("SliceFloat64").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new DoubleType(), false), new SimpleType.ArrayType(SimpleType.fromString("double")), SimpleType.dtypeFromString("double"), cache, 0, 100, slim, null);
        for (int i = 0;  i < 100;  i++) {
            ColumnarArray event = result.getArray(i);
            assertEquals(event.numElements(), i % 10);
            for (int j = 0;  j < i % 10;  j++) {
                assertEquals(event.getDouble(j), i, 0.0001);
            }
        }
        result.close();
    }

    /*
     *  +----------------------------------------+
        |long                                    |
        +----------------------------------------+
        |[0]                                     |
        |[10, 11]                                |
        |[20, 21, 22]                            |
        |[30, 31, 32, 33]                        |
        |[40, 41, 42, 43, 44]                    |
        |[50, 51, 52, 53, 54, 55]                |
        |[60, 61, 62, 63, 64, 65, 66]            |
        |[70, 71, 72, 73, 74, 75, 76, 77]        |
        |[80, 81, 82, 83, 84, 85, 86, 87, 88]    |
        |[90, 91, 92, 93, 94, 95, 96, 97, 98, 99]|
        +----------------------------------------+
     */
    @Test
    public void testBasketingAroundVector() throws IOException {
        TFile file = TFile.getFromFile("testdata/stdvector.root");
        TTree tree = new TTree(file.getProxy("tvec"), file);
        TBranch branch = tree.getBranches("long").get(0);
        BasketCache cache = BasketCache.getCache();
        SlimTBranchInterface slim = SlimTBranch.getFromTBranch(branch);

        for (int start = 0; start < 10; start += 1) {
            for (int end = start + 1; end <= 10; end += 1) {
                TTreeColumnVector result = new TTreeColumnVector(new ArrayType(new LongType(), false), new SimpleType.ArrayType(SimpleType.fromString("long")), SimpleType.dtypeFromString("long"), cache, start, end, slim, null);
                for (int i = start; i < end; i += 1) {
                    ColumnarArray event = result.getArray(i - start);
                    assertEquals(i + 1, event.numElements());
                    for (int j = 0; j <= i; j += 1) {
                        assertEquals(i * 10 + j, event.getLong(j));
                    }
                }
                result.close();
            }
        }
    }
}
