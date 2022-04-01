package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.logging.log4j.LogManager;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.Array.NIOBuf;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.JaggedArray;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray.Int4;
import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsJagged;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFileCache;
public class TTreeColumnVector extends ColumnVector {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();
    private long [] basketEntryOffsets;
    private ArrayBuilder.GetBasket getbasket;
    private ArrayBuilder builder;
    TBranch.ArrayDescriptor desc;
    private Array backing;
    private int entryStart;
    private int entryStop;
    SimpleType rootType;
    private DataType sparkType;
    private Dtype dtype;
    static private RootAllocator rootArrowAllocator = ArrowUtils.rootAllocator();
    private BufferAllocator allocator;
    static int totvec = 0;
    int vecid;
    public TTreeColumnVector(DataType type, SimpleType rootType, Dtype dtype, BasketCache basketCache, long entrystart, long entrystop, SlimTBranchInterface slimBranch, ThreadPoolExecutor executor, ROOTFileCache fileCache) {
        super(type);
        vecid = totvec;
        totvec += 1;
        this.basketEntryOffsets = slimBranch.getBasketEntryOffsets();
        this.getbasket = slimBranch.getArrayBranchCallback(basketCache, fileCache);
        this.entryStart = (int)entrystart;
        this.entryStop = (int)entrystop;
        this.rootType = rootType;
        this.sparkType = type;
        this.dtype = dtype;

        desc = slimBranch.getArrayDesc();
        if (desc == null) {
            interpretation = new AsDtype(dtype);
            this.builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, executor, entrystart, entrystop);
        } else if (desc.isFixed()) {
            interpretation = new AsDtype(dtype, Arrays.asList(desc.getFixedLength()));
            this.builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, executor, entrystart, entrystop);
        } else {
            interpretation = new AsJagged(new AsDtype(dtype), desc.getSkipBytes());
            this.builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, executor, entrystart, entrystop);
        }
//       ensureLoaded();
//        SparkEnv se = SparkEnv.get();
//        UnifiedMemoryManager mm = (UnifiedMemoryManager) SparkEnv.get().memoryManager();
//        mm.acquireExecutionMemory(0, 0, MemoryMode.OFF_HEAP);
//        mm.offHeapExecutionMemoryPool();
//        TaskContext tc = TaskContext.get();
//        ColumnarRow cc;
//        HashJoin ss = new HashJoin();

    }

    @Override
    public void close() {
        if (allocator != null) {
            allocator.close();
        }
    }

    private static final int SHORT_SIZE = Short.BYTES;
    private static final int INT_SIZE = Integer.BYTES;
    private static final int LONG_SIZE = Long.BYTES;
    private static final int FLOAT_SIZE = Float.BYTES;
    private static final int DOUBLE_SIZE = Double.BYTES;
    private static final int BYTE_SIZE = Byte.BYTES;
    private Interpretation interpretation;
    private static final long batchFlipShortEndianness(long t) {
        return (((t >> 0)  & 0x00FF00FF00FF00FFL) << 8 |
                ((t >> 8)  & 0x00FF00FF00FF00FFL) << 0  );
    }
    private static final short singleFlipShortEndianness(short t) {
        return (short) (((t >> 0)  & 0x00FF) << 8 |
                        ((t >> 8)  & 0x00FF) << 0  );
    }

    public static long testBatchFlipShortEndianness(long t) {
        return batchFlipShortEndianness(t);
    }
    public static short testSingleFlipShortEndianness(short t) {
        return singleFlipShortEndianness(t);
    }

    private static final long batchFlipIntEndianness(long t) {
        return (((t >> 0)  & 0x000000FF000000FFL) << 24 |
                ((t >> 8)  & 0x000000FF000000FFL) << 16 |
                ((t >> 16) & 0x000000FF000000FFL) << 8  |
                ((t >> 24) & 0x000000FF000000FFL) << 0   );
    }
    private static final int singleFlipIntEndianness(int t) {
        return (((t >> 0)  & 0x000000FF) << 24 |
                ((t >> 8)  & 0x000000FF) << 16 |
                ((t >> 16) & 0x000000FF) << 8  |
                ((t >> 24) & 0x000000FF) << 0   );
    }

    public static long testBatchFlipIntEndianness(long t) {
        return batchFlipIntEndianness(t);
    }
    public static int testSingleFlipIntEndianness(int t) {
        return singleFlipIntEndianness(t);
    }

    private static final long flipLongEndianness(long t) {
        return (((t >>  0)  & 0x00000000000000FFL) << 56 |
                ((t >>  8)  & 0x00000000000000FFL) << 48 |
                ((t >> 16)  & 0x00000000000000FFL) << 40 |
                ((t >> 24)  & 0x00000000000000FFL) << 32 |
                ((t >> 32)  & 0x00000000000000FFL) << 24 |
                ((t >> 40)  & 0x00000000000000FFL) << 16 |
                ((t >> 48)  & 0x00000000000000FFL) <<  8 |
                ((t >> 56)  & 0x00000000000000FFL) <<  0  );
    }

    public static long testFlipLongEndianness(long t) {
        return flipLongEndianness(t);
    }


    @SuppressWarnings("restriction")
    private void swapShortEndianness(ArrowBuf in, long index, int count) {
        long length = count * INT_SIZE;
        long curAddress = in.memoryAddress() + index;
        // copy word at a time
        while (length - 128 >= LONG_SIZE) {
            for (int x = 0; x < 16; x++) {
                long t = MemoryUtil.UNSAFE.getLong(curAddress);
                long v = batchFlipIntEndianness(t);
                MemoryUtil.UNSAFE.putLong(curAddress, v);
                length -= LONG_SIZE;
                curAddress += LONG_SIZE;
            }
        }
        while (length >= LONG_SIZE) {
            long t = MemoryUtil.UNSAFE.getLong(curAddress);
            long v = batchFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putLong(curAddress, v);
            length -= LONG_SIZE;
            curAddress += LONG_SIZE;
        }
        // copy last byte
        while (length > 0) {
            int t = MemoryUtil.UNSAFE.getInt(curAddress);
            int v = singleFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putInt(curAddress, v);
            length -= INT_SIZE;
            curAddress += INT_SIZE;
        }
    }


    @SuppressWarnings("restriction")
    private void swapIntEndianness(ArrowBuf in, long index, int count) {
        long length = count * INT_SIZE;
        long curAddress = in.memoryAddress() + index;
        // copy word at a time
        while (length - 128 >= LONG_SIZE) {
            for (int x = 0; x < 16; x++) {
                long t = MemoryUtil.UNSAFE.getLong(curAddress);
                long v = batchFlipIntEndianness(t);
                MemoryUtil.UNSAFE.putLong(curAddress, v);
                length -= LONG_SIZE;
                curAddress += LONG_SIZE;
            }
        }
        while (length >= LONG_SIZE) {
            long t = MemoryUtil.UNSAFE.getLong(curAddress);
            long v = batchFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putLong(curAddress, v);
            length -= LONG_SIZE;
            curAddress += LONG_SIZE;
        }
        // copy last byte
        while (length > 0) {
            int t = MemoryUtil.UNSAFE.getInt(curAddress);
            int v = singleFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putInt(curAddress, v);
            length -= INT_SIZE;
            curAddress += INT_SIZE;
        }
    }

    @SuppressWarnings("restriction")
    private void swapLongEndianness(ArrowBuf in, long index, int count) {
        long length = count * INT_SIZE;
        long curAddress = in.memoryAddress() + index;
        // copy word at a time
        while (length - 128 >= LONG_SIZE) {
            for (int x = 0; x < 16; x++) {
                long t = MemoryUtil.UNSAFE.getLong(curAddress);
                long v = batchFlipIntEndianness(t);
                MemoryUtil.UNSAFE.putLong(curAddress, v);
                length -= LONG_SIZE;
                curAddress += LONG_SIZE;
            }
        }
        while (length >= LONG_SIZE) {
            long t = MemoryUtil.UNSAFE.getLong(curAddress);
            long v = batchFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putLong(curAddress, v);
            length -= LONG_SIZE;
            curAddress += LONG_SIZE;
        }
        // copy last byte
        while (length > 0) {
            int t = MemoryUtil.UNSAFE.getInt(curAddress);
            int v = singleFlipIntEndianness(t);
            MemoryUtil.UNSAFE.putInt(curAddress, v);
            length -= INT_SIZE;
            curAddress += INT_SIZE;
        }
    }


    public synchronized ArrowColumnVector toArrowVector() {
        ensureLoaded();
        if (allocator == null) {
            allocator = rootArrowAllocator.newChildAllocator("TTreeColumnVector(" + vecid + ")", 0, Long.MAX_VALUE);
        }
        if (desc == null) {
            return toArrowScalarVector();
        } else if (desc.isFixed()) {
            return toArrowFixedLenVector();
        } else {
            return toArrowJaggedVector();
        }
    }

    private ArrowColumnVector toArrowScalarVector() {
        PrimitiveArray backing = (PrimitiveArray) this.backing;
        ArrowBuf contents = convertLaurelinBufToArrowBuf(backing);
        ArrowType arrowType = dtypeToArrow();

        FieldType arrowField = new FieldType(false, arrowType, null);

        ArrowFieldNode arrowNode = new ArrowFieldNode(backing.length(), 0);
        FieldVector field = arrowField.createNewSingleVector("testvec", allocator, null);
        field.loadFieldBuffers(arrowNode, Arrays.asList(null, contents));
        contents.close();
        return new ArrowColumnVector(field);
    }

    private ArrowColumnVector toArrowFixedLenVector2() {
        PrimitiveArray backing = (PrimitiveArray) this.backing;
        ArrowBuf contents = convertLaurelinBufToArrowBuf(backing);

        ArrowType listType = new ArrowType.FixedSizeList(desc.getFixedLength());
        FieldType listField = new FieldType(false, listType, null);
        ArrowFieldNode listNode = new ArrowFieldNode(contents.capacity(), 0);

        FixedSizeListVector arrowVec = FixedSizeListVector.empty("fixed len", desc.getFixedLength(), allocator);

        ArrowType arrowType = dtypeToArrow();
        FieldType arrowField = new FieldType(false, arrowType, null);
        ArrowFieldNode arrowNode = new ArrowFieldNode(backing.length(), 0);

        AddOrGetResult<ValueVector> children = arrowVec.addOrGetVector(arrowField);

        FieldVector field = (FieldVector) children.getVector();
        field.loadFieldBuffers(arrowNode, Arrays.asList(null, contents));

        contents.close();
        return new ArrowColumnVector(arrowVec);
    }

    private ArrowColumnVector toArrowFixedLenVector() {
        PrimitiveArray contentTemp = (PrimitiveArray) this.backing;
        int itemSize = new AsDtype(dtype).memory_itemsize();

        int countBufferSize = (entryStop - entryStart + 1) * INT_SIZE;

        ArrowBuf countsBuf = allocator.buffer(countBufferSize);
        String buf = "count[";
        for (int x = 0; x < (entryStop - entryStart + 1); x++) {
            countsBuf.writeInt(x * desc.getFixedLength());
            buf += "" + x * desc.getFixedLength() + ",";
        }
        buf += "]";
        System.out.println(buf);

        ArrowBuf contentBuf = convertLaurelinBufToArrowBuf(contentTemp);

        ArrowType outerType = new ArrowType.List();
        ArrowType innerType = dtypeToArrow();

        FieldType outerField = new FieldType(false, outerType, null);
        FieldType innerField = new FieldType(false, innerType, null);

        int outerLen = (entryStop - entryStart) * contentTemp.multiplicity();
        int innerLen = contentTemp.numitems();
        ArrowFieldNode outerNode = new ArrowFieldNode(outerLen, 0);
        ArrowFieldNode innerNode = new ArrowFieldNode(innerLen, 0);


        ListVector arrowVec = ListVector.empty("testcol", allocator);
        arrowVec.loadFieldBuffers(outerNode, Arrays.asList(null, countsBuf));

        AddOrGetResult<ValueVector> children = arrowVec.addOrGetVector(innerField);

        FieldVector innerVec = (FieldVector) children.getVector();
        innerVec.loadFieldBuffers(innerNode, Arrays.asList(null, contentBuf));

        countsBuf.getReferenceManager().release();
        contentBuf.getReferenceManager().release();
        return new ArrowColumnVector(arrowVec);
    }

    private ArrowColumnVector toArrowJaggedVector() {
        JaggedArray backing = (JaggedArray) this.backing;
        Int4 countsTmp = backing.offsets();
        int topCounter = countsTmp.get(0);
        PrimitiveArray contentTmp = (PrimitiveArray) backing.content();
        int itemSize = new AsDtype(dtype).memory_itemsize();

        ArrowBuf countsBuf = allocator.buffer(countsTmp.length() * INT_SIZE);

        ArrowBuf contentBuf = convertLaurelinBufToArrowBuf(contentTmp);

        countsBuf.setBytes(0, ((NIOBuf) countsTmp.raw()).toByteBuffer());
        swapIntEndianness(countsBuf, 0, countsTmp.length());

        ArrowType outerType = new ArrowType.List();
        ArrowType innerType = dtypeToArrow();

        FieldType outerField = new FieldType(false, outerType, null);
        FieldType innerField = new FieldType(false, innerType, null);

        ArrowFieldNode outerNode = new ArrowFieldNode(entryStop - entryStart, 0);
        ArrowFieldNode innerNode = new ArrowFieldNode(contentTmp.length(), 0);


        ListVector arrowVec = ListVector.empty("testcol", allocator);
        arrowVec.loadFieldBuffers(outerNode, Arrays.asList(null, countsBuf));

        AddOrGetResult<ValueVector> children = arrowVec.addOrGetVector(innerField);

        FieldVector innerVec = (FieldVector) children.getVector();
        innerVec.loadFieldBuffers(innerNode, Arrays.asList(null, contentBuf));

        countsBuf.getReferenceManager().release();
        contentBuf.getReferenceManager().release();
        return new ArrowColumnVector(arrowVec);
    }

    ArrowType dtypeToArrow() {
        if (dtype.name().equals("BOOL")) {
            return new ArrowType.Bool();
        } else if (dtype.name().equals("INT1")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("UINT1")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("INT2")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("UINT2")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("INT4")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("UINT4")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("INT8")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("UINT8")) {
            return new ArrowType.Int(AsDtype.memory_itemsize(dtype) * 8, true);
        } else if (dtype.name().equals("FLOAT4")) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (dtype.name().equals("FLOAT8")) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else {
            throw new AssertionError("Unrecognized dtype: " + dtype.name());
        }
    }

    public static byte testBatchByteVecToBitVec(long t) {
        return batchByteVecToBitVec(t);
    }

    private static final byte batchByteVecToBitVec(long t) {
        /*
         * Note that the "lowest" part of the long fills
         * the "highest" bit in the result. This has to do
         * with an implicit endianness swap when moving to
         * an arrow buffer
         */
        byte b1 = (byte) ((((t >> 0)) & 0x1) << 7);
        byte b2 = (byte) ((((t >> 8)) & 0x1) << 6);
        byte b3 = (byte) ((((t >> 16)) & 0x1) << 5);
        byte b4 = (byte) ((((t >> 24)) & 0x1) << 4);
        byte b5 = (byte) ((((t >> 32)) & 0x1) << 3);
        byte b6 = (byte) ((((t >> 40)) & 0x1) << 2);
        byte b7 = (byte) ((((t >> 48)) & 0x1) << 1);
        byte b8 = (byte) ((((t >> 56)) & 0x1) << 0);
        byte ret =  (byte) (b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8);
        int tmp = ret & 0xff;
//        String dump = String.format("%8s", Integer.toBinaryString(tmp));
//        System.out.println("B " + dump);
        return ret;
    }

    public static long testChunkByteVecToBitVec(long t1, long t2, long t3, long t4, long t5, long t6, long t7, long t8) {
        return chunkByteVecToBitVec(t1,t2,t3,t4,t5,t6,t7,t8);
    }

    /**
     * Consumes 8 true/false longs and produces a long bitvector with their contents
     */
    private static final long chunkByteVecToBitVec(long t1, long t2, long t3, long t4, long t5, long t6, long t7, long t8) {
        /*
         * Note that the "lowest" part of the long fills
         * the "highest" bit in the result. This has to do
         * with an implicit endianness swap when moving to
         * an arrow buffer.
         *
         * The casts are necessary to keep java from doing an
         * implicit converstion to int for some values, which
         * gets screwed up when the sign bit is "on"
         */
        long l1 = ((long) batchByteVecToBitVec(t1) <<  0L) & 0x00000000000000FFL;
        long l2 = ((long) batchByteVecToBitVec(t2) <<  8L) & 0x000000000000FF00L;
        long l3 = ((long) batchByteVecToBitVec(t3) << 16L) & 0x0000000000FF0000L;
        long l4 = ((long) batchByteVecToBitVec(t4) << 24L) & 0x00000000FF000000L;
        long l5 = ((long) batchByteVecToBitVec(t5) << 32L) & 0x000000FF00000000L;
        long l6 = ((long) batchByteVecToBitVec(t6) << 40L) & 0x0000FF0000000000L;
        long l7 = ((long) batchByteVecToBitVec(t7) << 48L) & 0x00FF000000000000L;
        long l8 = ((long) batchByteVecToBitVec(t8) << 56L) & 0xFF00000000000000L;
        long ret = l1 | l2 | l3 | l4 | l5 | l6 | l7 | l8;
//        String dump;
//        dump = String.format("%64s", Long.toBinaryString(l1));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l2));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l3));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l4));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l5));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l6));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l7));
//        System.out.println("L " + dump);
//        dump = String.format("%64s", Long.toBinaryString(l8));
//        System.out.println("L " + dump);
//
//        dump = String.format("%64s", Long.toBinaryString(ret));
//        System.out.println("C " + dump);
        return ret;
    }

    private static final ArrowBuf byteVectorToBitVector(PrimitiveArray content, ArrowBuf out) {
        // Position in bytes
        int bytepos = content.raw().position();
        int pos = bytepos;
        // Number of bytes remaining
        long length = (content.length() - pos);
        long curAddress = out.memoryAddress();

//        // copy word at a time
//        while (length - (16 * LONG_SIZE) >= LONG_SIZE) {
//            for (int x = 0; x < 16; x++) {
////                long v = chunkByteVecToBitVec(MemoryUtil.UNSAFE.getLong(curAddress),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 2 * LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 3 * LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 4 * LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 5 * LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 6 * LONG_SIZE),
////                                              MemoryUtil.UNSAFE.getLong(curAddress + 7 * LONG_SIZE));
//                long v = chunkByteVecToBitVec(content.rawarray().getLong(off),
//                                              content.rawarray().getLong(off + 1),
//                                              content.rawarray().getLong(off + 2),
//                                              content.rawarray().getLong(off + 3),
//                                              content.rawarray().getLong(off + 4),
//                                              content.rawarray().getLong(off + 5),
//                                              content.rawarray().getLong(off + 6),
//                                              content.rawarray().getLong(off + 7));
//
//                MemoryUtil.UNSAFE.putLong(curAddress, v);
//                length -= LONG_SIZE;
//                curAddress += LONG_SIZE;
//                off += LONG_SIZE;
//            }
//        }
        // We're reading 8 longs at once, then writing out a single long to the output
        while (length >= (LONG_SIZE * 8)) {
            long v = chunkByteVecToBitVec(content.raw().getLong(pos),
                    content.raw().getLong(pos + 1 * LONG_SIZE),
                    content.raw().getLong(pos + 2 * LONG_SIZE),
                    content.raw().getLong(pos + 3 * LONG_SIZE),
                    content.raw().getLong(pos + 4 * LONG_SIZE),
                    content.raw().getLong(pos + 5 * LONG_SIZE),
                    content.raw().getLong(pos + 6 * LONG_SIZE),
                    content.raw().getLong(pos + 7 * LONG_SIZE));
            MemoryUtil.UNSAFE.putLong(curAddress, v);
            length -= 8 * LONG_SIZE;
            curAddress += LONG_SIZE;
            // We read 8 longs
            pos += 8 * LONG_SIZE;
        }

        // Read a single long and return a single byte
        while (length >= LONG_SIZE) {
            long in = content.raw().getLong(pos);
            byte v = batchByteVecToBitVec(in);
            MemoryUtil.UNSAFE.putByte(curAddress, v);
            length -= LONG_SIZE;
            curAddress += BYTE_SIZE;
            pos += LONG_SIZE;
        }

        // copy last bytes
        while (length > 0) {
            byte v = 0;
            for (int x = 0; (x < 8) && (length > 0); ++x) {
                byte t = content.raw().get(pos);
                boolean r = (t == 1);
                v |= (r) ? (1 << x) : 0;
                length -= BYTE_SIZE;
                pos += BYTE_SIZE;
            }
            MemoryUtil.UNSAFE.putByte(curAddress, v);
            pos += 1;
        }
        return out;

    }

    /**
     * Converts our internal PrimitiveArray to the properly formatted ArrowBuf
     *
     * @param content
     * @return New ArrowBuf owned by caller
     */
    private ArrowBuf convertLaurelinBufToArrowBuf(PrimitiveArray content) {
        if (dtype.name().equals("BOOL")) {
            //While ROOT encodes booleans as 1 byte, Arrow puts them into bitvectors
            ArrowBuf contentBuf = allocator.buffer(content.length() / 8 + 1);
            byteVectorToBitVector(content, contentBuf);
            return contentBuf;
        } else {
            int itemSize = new AsDtype(dtype).memory_itemsize();
            int contentBufSize = content.numitems() * itemSize;
            ArrowBuf contentBuf = allocator.buffer(contentBufSize);

            contentBuf.setBytes(0, ((NIOBuf) content.raw()).toByteBuffer());
            if (dtype.name().equals("INT2")) {
                swapShortEndianness(contentBuf, 0, content.length());
            } else if (dtype.name().equals("UINT2") || dtype.name().equals("INT4")) {
                swapIntEndianness(contentBuf, 0, content.length());
            } else if (dtype.name().equals("UINT4") || dtype.name().equals("INT8") || dtype.name().equals("UINT8")) {
                swapLongEndianness(contentBuf, 0, content.length());
            }
            return contentBuf;
        }
    }

    public TTreeColumnVector(DataType type, SimpleType rootType, Dtype dtype, BasketCache basketCache, long entrystart, long entrystop, SlimTBranchInterface slimBranch, ThreadPoolExecutor executor) {
        this(type, rootType, dtype, basketCache, entrystart, entrystop, slimBranch, executor, (ROOTFileCache) null);
    }



    @Override
    public boolean hasNull() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int numNulls() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isNullAt(int rowId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return ((PrimitiveArray.Bool) backing).toBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return ((PrimitiveArray.Int1) backing).toByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return ((PrimitiveArray.Int2) backing).toShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return ((PrimitiveArray.Int4) backing).toInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return ((PrimitiveArray.Int8) backing).toLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return ((PrimitiveArray.Float4) backing).toFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return ((PrimitiveArray.Float8) backing).toDouble(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        Array array = builder.getArray(rowId, 1);
        Array subarray = array.subarray();
        ArrayColumnVector tmpvec = new ArrayColumnVector(((ArrayType)dataType()).elementType(), subarray);
        return new ColumnarArray(tmpvec, 0, subarray.length());
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getBinary(int rowId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * Return
     */

    @Override
    public boolean[] getBooleans(int rowId, int count) {
        byte[] tmp = (byte[])builder.getArray(rowId, count).toArray();
        boolean[] ret = new boolean[count];
        for (int i = 0; i < count; i += 1) {
            ret[i] = (tmp[i] == 1);
        }
        return ret;
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        return (byte[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        return (short[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public int[] getInts(int rowId, int count) {
        return (int[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        return (long[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        return (float[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        return (double[])(builder.getArray(rowId, count).toArray());
    }

    public void ensureLoaded() {
        backing = builder.getArray(0, entryStop - entryStart);
    }
}
