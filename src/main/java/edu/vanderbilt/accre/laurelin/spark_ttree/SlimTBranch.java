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
     * Deduplicate range->basket maps, since many (all?) of them will be same
     * for different branches in a file
     */
    private static HashMap<ImmutableRangeMap<Long, Integer>,
                           ImmutableRangeMap<Long, Integer>> dedupRangeMap =
                           new HashMap<ImmutableRangeMap<Long, Integer>,
                                       ImmutableRangeMap<Long, Integer>>();

    public synchronized ImmutableRangeMap<Long, Integer> dedupAndReturnRangeMap(ImmutableRangeMap<Long, Integer> val) {
        dedupRangeMap.putIfAbsent(val, val);
        return dedupRangeMap.get(val);
    }

    /**
     * Copy the given slim branch and trim it by removing unneccessary basket
     * information
     *
     * @param eventStart the zeroth event we want to read
     * @param eventEnd the event past the last event we want to read
     * @return slimtbranch with only these events stored
     */

    public SlimTBranch copyAndTrim(long eventStart, long eventEnd) {
        SlimTBranch ret = new SlimTBranch(path, this.getBasketEntryOffsets(), arrayDesc);
        ImmutableRangeMap<Long, Integer> overlap = entryOffsetToRangeMap(this.getBasketEntryOffsets(), eventStart, eventEnd);

        for (Entry<Range<Long>, Integer> e: overlap.asMapOfRanges().entrySet()) {
            ret.addBasket(e.getValue(), baskets.get(e.getValue()));
        }
        ret.basketStart = rangeToBasketIDMap.get(eventStart);
        ret.basketEnd = rangeToBasketIDMap.get(eventEnd - 1) + 1;
        ret.checkInvariants();
        return ret;
    }

    public void checkInvariants() {
        if (basketEnd == 0) {
            assert basketEnd != 0;
        }
    }

    public SlimTBranch(String path, long []basketEntryOffsets, TBranch.ArrayDescriptor desc) {
        this(path, basketEntryOffsets, desc, 0);
        checkInvariants();
    }

    private static Range<Long>[] entryOffsetToRangeArray(long[] basketEntryOffsets) {
        @SuppressWarnings("unchecked")
        Range<Long>[] ret = new Range[basketEntryOffsets.length - 1];
        for (int i = 0; i < basketEntryOffsets.length - 1; i += 1) {
            Range<Long> range = Range.closed(basketEntryOffsets[i], basketEntryOffsets[i + 1] - 1);
            ret[i] = range;
        }
        return ret;
    }

    private SlimTBranch(String path, long []basketEntryOffsets, TBranch.ArrayDescriptor desc, int basketStart) {
        this(path, entryOffsetToRangeArray(basketEntryOffsets), desc, basketStart);
        checkInvariants();
    }

    public SlimTBranch(String path, Range<Long>[] basketRangeList, TBranch.ArrayDescriptor desc) {
        this(path, basketRangeList, desc, 0);
        checkInvariants();
    }

    public SlimTBranch(String path, Range<Long>[] basketRangeList, TBranch.ArrayDescriptor desc, int basketStart) {
        this.path = path;
        this.arrayDesc = desc;
        this.baskets = new HashMap<Integer, SlimTBasket>();
        this.basketStart = basketStart;
        this.basketEnd = basketRangeList.length + basketStart;

        Builder<Long, Integer> basketBuilder = new ImmutableRangeMap.Builder<Long, Integer>();
        for (int i = 0; i < basketRangeList.length; i += 1) {
            int targetBasket = i + basketStart;
            basketBuilder = basketBuilder.put(basketRangeList[i], targetBasket);
        }
        rangeToBasketIDMap = dedupAndReturnRangeMap(basketBuilder.build());
        checkInvariants();
    }

    public static SlimTBranch getFromTBranch(TBranch fatBranch) {
        SlimTBranch slimBranch = new SlimTBranch(fatBranch.getTree().getBackingFile().getFileName(), fatBranch.getBasketEntryOffsets(), fatBranch.getArrayDescriptor());
        for (int i = 0; i < fatBranch.getBasketCount(); i += 1) {
            SlimTBasket slimBasket = SlimTBasket.makeLazyBasket(fatBranch.getBasketSeek()[i]);
            slimBranch.addBasket(i, slimBasket);
        }
        return slimBranch;
    }

    @Override
    public ImmutableRangeMap<Long, Integer> getRangeToBasketIDMap() {
        return rangeToBasketIDMap;
    }

    private long[] cachedBasketEntry = null;

    @Override
    public synchronized long [] getBasketEntryOffsets() {
        if (cachedBasketEntry != null) {
            return cachedBasketEntry;
        }
        ImmutableMap<Range<Long>, Integer> descMap = getRangeToBasketIDMap().asMapOfRanges();
        int maxIdx = Math.max(basketEntryOffsetsLength, basketEnd);
        cachedBasketEntry = new long[maxIdx + 1];
        for (int i = 0; i < basketStart; i += 1) {
            cachedBasketEntry[i] = i;
        }
        long topMost = 0;
        for (Entry<Range<Long>, Integer> e: descMap.entrySet()) {
            int idx = e.getValue();
            if ((idx < basketStart) || (idx >= basketEnd)) {
                continue;
            }
            cachedBasketEntry[idx] = e.getKey().lowerEndpoint();
            cachedBasketEntry[idx + 1] = e.getKey().upperEndpoint() + 1;
            topMost = e.getKey().upperEndpoint() + 1;
        }
        for (int i = basketEnd + 2; i < maxIdx; i += 1) {
            cachedBasketEntry[i] = topMost + i;
        }
        if (cachedBasketEntry[0] != 0) {
            assert cachedBasketEntry[0] == 0;
        }
        return cachedBasketEntry;
    }

    @Override
    public SlimTBasket getBasket(int basketid) {
        SlimTBasket ret = baskets.get(basketid);
        if (ret == null) {
            throw new IndexOutOfBoundsException("Tried to get nonexistent basket: " + basketid);
        }
        return baskets.get(basketid);
    }

    public int getStoredBasketCount() {
        return baskets.size();
    }

    public void addBasket(int idx, SlimTBasket basket) {
        baskets.put(idx, basket);
    }

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

        private SlimTBasket(long offset) {
            this.offset = offset;
        }

        public static SlimTBasket makeEagerBasket(SlimTBranchInterface branch, long offset, int compressedLen, int uncompressedLen, int keyLen, int last) {
            SlimTBasket ret = new SlimTBasket(offset);
            ret.isPopulated = true;
            ret.compressedLen = compressedLen;
            ret.uncompressedLen = uncompressedLen;
            ret.keyLen = keyLen;
            ret.last = last;
            return ret;
        }

        public static SlimTBasket makeLazyBasket(long offset) {
            SlimTBasket ret = new SlimTBasket(offset);
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
