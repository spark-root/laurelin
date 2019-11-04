package edu.vanderbilt.accre.spark_ttree;

import static edu.vanderbilt.accre.Helpers.getBigTestDataIfExists;
import static org.junit.Assert.assertArrayEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.junit.Test;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.Range;

import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.CacheStash;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;
import edu.vanderbilt.accre.laurelin.root_proxy.SimpleType;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch.ArrayDescriptor;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch.SlimTBasket;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranchInterface;
import edu.vanderbilt.accre.laurelin.spark_ttree.TTreeColumnVector;

public class TTreeColumnVectorTest {
    byte[] tmp = new byte []{127,-1,0,1};
    Integer[] tmp2 = new Integer [] {1,2,3};
    Cache basketCache = CacheStash.getCache();

    private static class SlimTBranchStub implements SlimTBranchInterface {
        /*
         * to get this to stub the array stuff, I need to (merely) implement
         *
         *    slimBranch.getArrayBranchCallback(basketCache, fileCache);
         *
         * since that's what passed into the inner ArrayBuilder stuff
         *
         */

        ArrayDescriptor desc;
        ArrayList<ByteBuffer> payload = new ArrayList<ByteBuffer>();
        ArrayList<Integer> fLast = new ArrayList<Integer>();
        ArrayList<Integer> fObjLen = new ArrayList<Integer>();
        int keyLen;
        long[] basketEntryOffsets;


        public SlimTBranchStub(byte[][] payload, byte [][] offsets, ArrayDescriptor desc, long[] basketEntryOffsets, int[] last, int keyLen) {
            this.desc = desc;
            this.basketEntryOffsets = basketEntryOffsets;
            this.keyLen = keyLen;
            for (int i = 0; i < payload.length; i += 1) {
                int len = payload[i].length;
                // len += keyLen;
                if (offsets != null) {
                    len += offsets[i].length;
                }
                ByteBuffer basketBuf = ByteBuffer.allocate(len);
                basketBuf.put(payload[i]);
                if (offsets != null) {
                    basketBuf.put(offsets[i]);
                }
                if (last != null) {
                    this.fLast.add(last[i]);
                } else {
                    this.fLast.add(payload[i].length);
                }
                this.fObjLen.add(len);
                this.payload.add(basketBuf);
            }
        }

        @Override
        public ArrayBuilder.GetBasket getArrayBranchCallback(Cache basketCache, ROOTFileCache fileCache) {
            return new BranchCallback(basketCache, this, fileCache);
        }

        class BranchCallback implements ArrayBuilder.GetBasket {
            Cache basketCache;
            SlimTBranchInterface branch;
            ROOTFileCache fileCache;

            public BranchCallback(Cache basketCache, SlimTBranchInterface branch, ROOTFileCache fileCache) {
                this.basketCache = basketCache;
                this.branch = branch;
                this.fileCache = fileCache;
            }

            @Override
            public ArrayBuilder.BasketKey basketkey(int basketid) {
                SlimTBasket basket = branch.getBasket(basketid);
                return new ArrayBuilder.BasketKey(basket.getKeyLen(), basket.getLast(), basket.getObjLen());
            }

            @Override
            public RawArray dataWithoutKey(int basketid) {
                return new RawArray(payload.get(basketid));
            }
        }

        @Override
        public long[] getBasketEntryOffsets() {
            return basketEntryOffsets;
        }

        @Override
        public SlimTBasket getBasket(int basketid) {
            int len = payload.get(basketid).limit();
            return SlimTBasket.makeEagerBasket(this, 0L, fObjLen.get(basketid), fObjLen.get(basketid),keyLen, fLast.get(basketid));
        }

        @Override
        public ArrayDescriptor getArrayDesc() {
            return desc;
        }

        @Override
        public ImmutableRangeMap<Long, Integer> getRangeToBasketIDMap() {
            Builder<Long, Integer> basketBuilder = new ImmutableRangeMap.Builder<Long, Integer>();
            for (int i = 0; i < basketEntryOffsets.length - 1; i += 1) {
                basketBuilder = basketBuilder.put(Range.closed(basketEntryOffsets[i], basketEntryOffsets[i + 1] - 1), i);
            }
            return basketBuilder.build();
        }
    }

    private TTreeColumnVector getDummyScalarVec() {
        byte[][] payload = {intToBytes(new Integer[] {0,1,2,3,4,5,6,7,8,9}),
                            intToBytes(new Integer[] {10,11,12,13,14,15,16,17,18,19}),
                            intToBytes(new Integer[] {20,21,22,23,24,25,26,27,28,29})};

        SlimTBranchInterface branch = new SlimTBranchStub(payload, null, null, new long[]{0,9,19,29}, null, 0);
        return new TTreeColumnVector(DataTypes.IntegerType, SimpleType.Int32, Dtype.INT4, basketCache, 2, 26, branch, null);
    }

    private TTreeColumnVector getDummyJaggedArrayVec(long entryStart, long entryStop) {
        // raw dump of testdata/all-types.root:Events/SliceI32, with the basket
        // copied 3 times to test partition edges
        byte[][] payload = {new byte[] {0,0,0,1,0,0,0,2,0,0,0,2,-128,0,0,1,-128,0,0,2,-128,0,0,2,127,-1,-1,-2,127,-1,-1,-3,127,-1,-1,-3,0,0,0,10,0,0,0,77,0,0,0,77,0,0,0,81,0,0,0,89,0,0,0,89,0,0,0,93,0,0,0,101,0,0,0,101,0,0,0,105,0,0,0,0},
                            new byte[] {0,0,0,1,0,0,0,2,0,0,0,2,-128,0,0,1,-128,0,0,2,-128,0,0,2,127,-1,-1,-2,127,-1,-1,-3,127,-1,-1,-3,0,0,0,10,0,0,0,77,0,0,0,77,0,0,0,81,0,0,0,89,0,0,0,89,0,0,0,93,0,0,0,101,0,0,0,101,0,0,0,105,0,0,0,0},
                            new byte[] {0,0,0,1,0,0,0,2,0,0,0,2,-128,0,0,1,-128,0,0,2,-128,0,0,2,127,-1,-1,-2,127,-1,-1,-3,127,-1,-1,-3,0,0,0,10,0,0,0,77,0,0,0,77,0,0,0,81,0,0,0,89,0,0,0,89,0,0,0,93,0,0,0,101,0,0,0,101,0,0,0,105,0,0,0,0}};

        ArrayDescriptor desc = ArrayDescriptor.newVarArray("dummyBranch");
        // The basket has fKeylen of 77, and fLast of 113
        SlimTBranchInterface branch = new SlimTBranchStub(payload, null, desc, new long[] {0,9,18,27}, new int[] {113,113,113}, 77);
        return new TTreeColumnVector(new ArrayType(new IntegerType(), false),
                                        new SimpleType.ArrayType(SimpleType.fromString("int")),
                                        SimpleType.dtypeFromString("int"),
                                        basketCache,
                                        entryStart,
                                        entryStop,
                                        branch,
                                        null);
    }

    private TTreeColumnVector getDummyJaggedArrayVec1basket(long entryStart, long entryStop) {
        // raw dump of testdata/all-types.root:Events/SliceI32, with the basket
        // copied 3 times to test partition edges
        byte[][] payload = {new byte[] {0,0,0,1,0,0,0,2,0,0,0,2,-128,0,0,1,-128,0,0,2,-128,0,0,2,127,-1,-1,-2,127,-1,-1,-3,127,-1,-1,-3,0,0,0,10,0,0,0,77,0,0,0,77,0,0,0,81,0,0,0,89,0,0,0,89,0,0,0,93,0,0,0,101,0,0,0,101,0,0,0,105,0,0,0,0}};

        ArrayDescriptor desc = ArrayDescriptor.newVarArray("dummyBranch");
        // The basket has fKeylen of 77, and fLast of 113
        SlimTBranchInterface branch = new SlimTBranchStub(payload, null, desc, new long[] {0,9}, new int[] {113}, 77);
        return new TTreeColumnVector(new ArrayType(new IntegerType(), false),
                                     new SimpleType.ArrayType(SimpleType.fromString("int")),
                                     SimpleType.dtypeFromString("int"),
                                        basketCache,
                                        entryStart,
                                        entryStop,
                                        branch,
                                        null);
    }

    @Test
    public void getJaggedArrayVec() {
        // Exhaustively test the entry start/stop
        for (int entrystart = 2; entrystart <= 27; entrystart += 1) {
            for (int entrystop = 10; entrystop <= 27; entrystop += 1) {
                //System.out.println("Reading from " + entrystart + " to " + entrystop);
                for (int i = 0; i < entrystop - entrystart; i += 1) {
                    TTreeColumnVector result = getDummyJaggedArrayVec(entrystart,entrystop);
                    Object[] event = result.getArray(i).array();
                    Integer[] truth = getDummyJaggedArrayTruth(i + entrystart);
                    String vals = "no match start/stop/i " + entrystart + "/" + entrystop + "/" + i + " truth: " + Arrays.toString(truth) + " event: " + Arrays.toString(event);
                    assertArrayEquals(vals, truth, event);
                    result.close();
                }
            }
        }
    }

    @Test
    public void getJaggedArrayVec1Basket() {
        // Exhaustively test the entry start/stop
        for (int entrystart = 0; entrystart <= 9; entrystart += 1) {
            for (int entrystop = entrystart + 1; entrystop <= 9; entrystop += 1) {
                TTreeColumnVector result = getDummyJaggedArrayVec1basket(entrystart, entrystop);
                for (int i = 0; i < entrystop - entrystart; i += 1) {
                    Object[] event = result.getArray(i).array();
                    Integer[] truth = getDummyJaggedArrayTruth(i + entrystart);
                    String vals = "no match start/stop/i " + entrystart + "/" + entrystop + "/" + i + " truth: " + Arrays.toString(truth) + " event: " + Arrays.toString(event);
                    assertArrayEquals(vals, truth, event);
                }
                result.close();
            }
        }
    }


    private Integer[] getDummyJaggedArrayTruth(int eventid) {
        // 9 events in the underlying basket
        int base = eventid % 9;
        switch (base) {
            case 0:
                return new Integer[] {};
            case 1:
                return new Integer[] {1};
            case 2:
                return new Integer[] {2,2};
            case 3:
                return new Integer[] {};
            case 4:
                return new Integer[] {-2147483647};
            case 5:
                return new Integer[] {-2147483646,-2147483646};
            case 6:
                return new Integer[] {};
            case 7:
                return new Integer[] {2147483646};
            case 8:
                return new Integer[] {2147483645,2147483645};
            default:
                throw new RuntimeException("Impossible");
        }
    }

//    @Test(expected = UnsupportedOperationException.class)
//    public void getmap_is_unimplemented() {
//        TTreeColumnVector vec = getDummyScalarVec();
//        vec.getMap(0);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void getdecimal_is_unimplemented() {
//        TTreeColumnVector vec = getDummyScalarVec();
//        vec.getDecimal(0, 1, 2);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void getutf8string_is_unimplemented() {
//        TTreeColumnVector vec = getDummyScalarVec();
//        vec.getUTF8String(0);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void getbinary_is_unimplemented() {
//        TTreeColumnVector vec = getDummyScalarVec();
//        vec.getBinary(0);
//    }

    @Test
    public void scalar_integer_should_parse() {
        Integer[] testInt = new Integer[] {1,2,3};
        byte[] testByte = intToBytes(testInt);
    }

    // There's gotta be a better way to do this with generics but...
    public byte[] intToBytes(Integer[] in) {
        int rawLen = in.length * 4;
        ByteBuffer tmp = ByteBuffer.allocate(rawLen);
        for (int i: in) {
            tmp.putInt(i);
        }
        return tmp.array();
    }

    @Test
    public void testThing() throws FileNotFoundException, IOException {
        String path = getBigTestDataIfExists("testdata/pristine/dump-nano-4-muon-pt.txt");
        ArrayList<float []> good = new ArrayList<float []>();
        good.ensureCapacity(50000);
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.equals("")) {
                    good.add(new float[0]);
                } else {
                    String[] vals = line.split(",");
                    float[] floats = new float[vals.length];
                    for (int i = 0; i < vals.length; i += 1) {
                        floats[i] = Float.parseFloat(vals[i]);
                    }
                    good.add(floats);
                }
            }
        }
    }
}
