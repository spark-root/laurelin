package edu.vanderbilt.accre.laurelin.spark_ttree;

import static edu.vanderbilt.accre.laurelin.root_proxy.TBranch.entryOffsetToRangeMap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.Range;

import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TKey;

/**
 * Contains all the info needed to read a TBranch and its constituent TBaskets
 * without needing to deserialize the ROOT metadata -- i.e. this contains paths
 * and byte offsets to each basket
 */
public class SlimTBranch implements Serializable, SlimTBranchInterface {
    private static final long serialVersionUID = 1L;
    private String path;

    /**
     * This value is marked transient to ensure that it is not sent through
     * the serialization machinery, with the ensuing bloat. Will eventually be
     * deleted
     */
    private transient long []basketEntryOffsets;

    private int basketEntryOffsetsLength;

    /**
     * This is our representation of the basketEntryOffsets array. This format
     * is much easier to trim off the unneeded bits.
     */
    private ImmutableRangeMap<Long, Integer> rangeToBasketIDMap;
    private Map<Integer, SlimTBasket> baskets;
    private TBranch.ArrayDescriptor arrayDesc;
    private int basketStart;
    private int basketEnd;

    /**
     *
     * @param eventStart the zeroth event we want to read
     * @param eventEnd the event past the last event we want to read
     * @return slimtbranch with only these events stored
     */

    public SlimTBranch copyAndTrim(long eventStart, long eventEnd) {
        System.out.println("Getting start " + eventStart + " end " + eventEnd);
        SlimTBranch ret = new SlimTBranch(path, basketEntryOffsets, arrayDesc);;
        ImmutableRangeMap<Long, Integer> overlap = entryOffsetToRangeMap(basketEntryOffsets, eventStart, eventEnd);
        ret.rangeToBasketIDMap = overlap;
        for (Entry<Range<Long>, Integer> e: overlap.asMapOfRanges().entrySet()) {
            ret.addBasket(e.getValue(), baskets.get(e.getValue()));
        }
        ret.basketStart = rangeToBasketIDMap.get(eventStart);
        ret.basketEnd = rangeToBasketIDMap.get(eventEnd - 1);
        return ret;
    }

    public SlimTBranch(String path, long []basketEntryOffsets, TBranch.ArrayDescriptor desc) {
        this.path = path;
        this.basketEntryOffsets = basketEntryOffsets;
        this.basketEntryOffsetsLength = basketEntryOffsets.length;
        this.baskets = new HashMap<Integer, SlimTBasket>();
        this.arrayDesc = desc;
        this.basketStart = 0;
        this.basketEnd = basketEntryOffsets.length;

        Builder<Long, Integer> basketBuilder = new ImmutableRangeMap.Builder<Long, Integer>();
        for (int i = 0; i < basketEntryOffsets.length - 1; i += 1) {
            basketBuilder = basketBuilder.put(Range.closed(basketEntryOffsets[i], basketEntryOffsets[i + 1] - 1), i);
        }
        rangeToBasketIDMap = basketBuilder.build();
    }

    public static SlimTBranch getFromTBranch(TBranch fatBranch) {
        SlimTBranch slimBranch = new SlimTBranch(fatBranch.getTree().getBackingFile().getFileName(), fatBranch.getBasketEntryOffsets(), fatBranch.getArrayDescriptor());
        for (int i = 0; i < fatBranch.getBasketCount(); i += 1) {
            SlimTBasket slimBasket = SlimTBasket.makeLazyBasket(slimBranch,
                    fatBranch.getBasketSeek()[i]);
            slimBranch.addBasket(i, slimBasket);
        }
        return slimBranch;
    }

    @Override
    public ImmutableRangeMap<Long, Integer> getRangeToBasketIDMap() {
        return rangeToBasketIDMap;
    }

    @Override
    public long [] getBasketEntryOffsets() {
        ImmutableMap<Range<Long>, Integer> descMap = rangeToBasketIDMap.asMapOfRanges();
        long[] tmpBasket = new long[basketEntryOffsetsLength];
        for (int i = 0; i < basketStart; i += 1) {
            tmpBasket[i] = 0;
        }
        long topMost = 0;
        for (Entry<Range<Long>, Integer> e: descMap.entrySet()) {
            tmpBasket[e.getValue()] = e.getKey().lowerEndpoint();
            tmpBasket[e.getValue() + 1] = e.getKey().upperEndpoint() + 1;
            topMost = e.getKey().upperEndpoint() + 1;
        }
        for (int i = basketEnd + 2; i < basketEntryOffsetsLength; i += 1) {
            tmpBasket[i] = topMost;
        }
        if ((basketEntryOffsets != null) && (basketStart == 0) && (basketEnd == basketEntryOffsetsLength)) {
            assert Arrays.equals(tmpBasket, basketEntryOffsets);
        }
        if (tmpBasket[0] != 0) {
            assert tmpBasket[0] == 0;
        }
        return tmpBasket;
    }

    @Override
    public SlimTBasket getBasket(int basketid) {
        SlimTBasket ret = baskets.get(basketid);
        if (ret == null) {
            throw new IndexOutOfBoundsException("Tried to get nonexistent basket: " + basketid);
        }
        return baskets.get(basketid);
    }

    public void addBasket(int idx, SlimTBasket basket) {
        baskets.put(idx, basket);
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public TBranch.ArrayDescriptor getArrayDesc() {
        return arrayDesc;
    }

    /**
     * Glue callback to integrate with edu.vanderbilt.accre.laurelin.array
     * @param basketCache the cache we should be using
     * @param fileCache storage for filehandles
     * @return GetBasket object used by array
     */
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

        private ROOTFile getBackingFile() throws IOException {
            ROOTFile tmpFile;
            if (fileCache == null) {
                tmpFile = ROOTFile.getInputFile(path);
            } else {
                tmpFile = fileCache.getROOTFile(path);
            }
            return tmpFile;
        }

        @Override
        public ArrayBuilder.BasketKey basketkey(int basketid) {
            SlimTBasket basket = branch.getBasket(basketid);
            try {
                ROOTFile tmpFile = getBackingFile();
                basket.initializeMetadata(tmpFile);
                return new ArrayBuilder.BasketKey(basket.getKeyLen(), basket.getLast(), basket.getObjLen());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public RawArray dataWithoutKey(int basketid) {
            SlimTBasket basket = branch.getBasket(basketid);
            try {
                ROOTFile tmpFile = getBackingFile();
                // the offset of each basket is guaranteed to be unique and
                // stable
                RawArray data = null;
                data = basketCache.get(tmpFile, basket.getOffset());
                if (data == null) {
                    data = new RawArray(basket.getPayload(tmpFile));
                    basketCache.put(tmpFile, basket.getOffset(), data);
                }
                return data;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SlimTBasket implements Serializable {
        private static final Logger logger = LogManager.getLogger();

        private static final long serialVersionUID = 1L;
        private SlimTBranchInterface branch;
        private long offset;
        private Cursor payload;


        private boolean isPopulated = false;
        private int compressedLen;
        private int uncompressedLen;
        private int keyLen;
        private int last;

        private short vers;
        private int fBufferSize;
        private int fNevBufSize;
        private int fNevBuf;
        private byte fHeaderOnly;
        private Cursor headerEnd;

        private SlimTBasket(SlimTBranchInterface slimBranch, long offset) {
            branch = slimBranch;
            this.offset = offset;
        }

        public static SlimTBasket makeEagerBasket(SlimTBranchInterface branch, long offset, int compressedLen, int uncompressedLen, int keyLen, int last) {
            SlimTBasket ret = new SlimTBasket(branch, offset);
            ret.isPopulated = true;
            ret.compressedLen = compressedLen;
            ret.uncompressedLen = uncompressedLen;
            ret.keyLen = keyLen;
            ret.last = last;
            return ret;
        }

        public static SlimTBasket makeLazyBasket(SlimTBranchInterface branch, long offset) {
            SlimTBasket ret = new SlimTBasket(branch, offset);
            ret.isPopulated = false;
            return ret;
        }

        public synchronized void initializeMetadata(ROOTFile tmpFile) {
            if (isPopulated == false) {
                try {
                    Cursor cursor = tmpFile.getCursor(offset);
                    TKey key = new TKey();
                    Cursor c = key.getFromFile(cursor);
                    keyLen = key.getKeyLen();
                    compressedLen = key.getNBytes() - key.getKeyLen();
                    uncompressedLen = key.getObjLen();
                    vers = c.readShort();
                    fBufferSize = c.readInt();
                    fNevBufSize = c.readInt();
                    fNevBuf = c.readInt();
                    last = c.readInt();
                    fHeaderOnly = c.readChar();
                    headerEnd = c;
                    isPopulated = true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public int getKeyLen() {
            if (isPopulated == false) {
                throw new RuntimeException("Slim basket not initialized");
            }
            return keyLen;
        }

        public int getObjLen() {
            if (isPopulated == false) {
                throw new RuntimeException("Slim basket not initialized");
            }
            return uncompressedLen;
        }

        public int getLast() {
            if (isPopulated == false) {
                throw new RuntimeException("Slim basket not initialized");
            }
            return last;
        }

        public long getOffset() {
            return offset;
        }

        public ByteBuffer getPayload(ROOTFile tmpFile) throws IOException {
            initializeMetadata(tmpFile);
            if (this.payload == null) {
                initializePayload(tmpFile);
            }
            return this.payload.readBuffer(0, uncompressedLen);
        }

        private void initializePayload(ROOTFile tmpFile) throws IOException {
            Cursor fileCursor = tmpFile.getCursor(offset);
            if (isPopulated == false) {
                throw new RuntimeException("Slim basket not initialized");
            }
            this.payload = fileCursor.getPossiblyCompressedSubcursor(keyLen,
                    compressedLen,
                    uncompressedLen,
                    keyLen);
        }
    }
}
