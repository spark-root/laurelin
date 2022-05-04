package edu.vanderbilt.accre.laurelin.spark_ttree;

import static com.google.common.base.Preconditions.checkNotNull;
import static edu.vanderbilt.accre.laurelin.root_proxy.TBranch.entryOffsetToRangeMap;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputValidation;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch.CompressedBasketInfo;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFile;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFileCache;

/**
 * Contains all the info needed to read a TBranch and its constituent TBaskets
 * without needing to deserialize the ROOT metadata -- i.e. this contains paths
 * and byte offsets to each basket
 *
 * ObjectInputValidation
 */
public class SlimTBranch implements Serializable, KryoSerializable, SlimTBranchInterface, ObjectInputValidation {
    private static final Logger logger = LogManager.getLogger();
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

    private static Interner<ImmutableRangeMap<Long, Integer>> rangeMapInterner = Interners.newWeakInterner();

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
        checkNotNull(rangeToBasketIDMap);
        if (basketEnd == 0) {
            assert basketEnd != 0;
        }
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

    public SlimTBranch(String path, long []basketEntryOffsets, TBranch.ArrayDescriptor desc) {
        this(path, basketEntryOffsets, desc, 0);
        checkInvariants();
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
    	this(path, basketRangeList, desc, 0, null);
    	checkInvariants();
    }

    public SlimTBranch(String path, Range<Long>[] basketRangeList, TBranch.ArrayDescriptor desc, int basketStart,
			CompressedBasketInfo[] compressedBasketInfo) {
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
        ImmutableRangeMap<Long, Integer> tmp = basketBuilder.build();
        checkNotNull(tmp);
        rangeToBasketIDMap = rangeMapInterner.intern(tmp);
        checkInvariants();
	}

	public static SlimTBranch getFromTBranch(TBranch fatBranch) {
        SlimTBranch slimBranch = new SlimTBranch(fatBranch.getTree().getBackingFile().getFileName(), fatBranch.getBasketEntryOffsets(), fatBranch.getArrayDescriptor());
        for (int i = 0; i < fatBranch.getBasketCount(); i += 1) {
            CompressedBasketInfo compressedBasketInfo = fatBranch.getCompressedBasketInfo()[i];
            SlimTBasket slimBasket = SlimTBasket.makeLazyBasket(fatBranch.getBasketSeek()[i], compressedBasketInfo);
            slimBranch.addBasket(i, slimBasket);
        }
        return slimBranch;
    }

    @Override
    public ImmutableRangeMap<Long, Integer> getRangeToBasketIDMap() {
        checkNotNull(rangeToBasketIDMap);
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
        return ret;
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
    public ArrayBuilder.GetBasket getArrayBranchCallback(BasketCache basketCache, ROOTFileCache fileCache) {
        return new BranchCallback(basketCache, this, fileCache);
    }

    class BranchCallback implements ArrayBuilder.GetBasket {
        BasketCache basketCache;
        SlimTBranchInterface branch;
        ROOTFileCache fileCache;

        public BranchCallback(BasketCache basketCache, SlimTBranchInterface branch, ROOTFileCache fileCache) {
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
                if (basket.getCompressedBasketInfo() == null) {
                    return new ArrayBuilder.BasketKey(basket.getKeyLen(), basket.getLast(), basket.getObjLen(), null);
                } else {
                    return new ArrayBuilder.BasketKey(basket.getKeyLen(), basket.getLast(), basket.getObjLen(), basket.getCompressedBasketInfo());
                }
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
                long parentOffset = -1;
                CompressedBasketInfo compressedInfo = basket.getCompressedBasketInfo();
                if (compressedInfo != null) {
                	parentOffset = compressedInfo.getCompressedParentOffset();
                }
                //data = basketCache.get(tmpFile, basket.getOffset(), parentOffset);
                //if (data == null) {
                    data = new RawArray(Array.wrap(basket.getPayload(tmpFile)));
                //    basketCache.put(tmpFile, basket.getOffset(), parentOffset, data);
                //}
                return data;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * The in-memory representation of a SlimTBranch is quite bulky, and this
     * space usage is multiplied by the # of branches and the # of partitions
     * being processed that needs to be transmitted from the driver to
     * executors. This repeated transmitting adds a decently-high per-partition
     * overhead. Though, the total size of all SlimTBranch for an entire
     * CMS NANOAOD file ends up being ~3MByte, so it's not so bad to have it
     * in-memory, it's repeatedly transmitting the 3MByte 10s of thousands of
     * times.
     *
     * <p>Instead of jumping through hoops to make the in-memory size really
     * compact, we can instead override Java's serialization mechanism to
     * provide an alternate representation used for (de-)serialization. The
     * SerializeStorage class is what's actually transmitted over the wire.
     *
     */
    public static class SerializeStorage implements Serializable, ObjectInputValidation {
        private static final long serialVersionUID = 1L;

        /**
         * Offset of this branch's baskets into the global basket list, the
         * transmitted basket information is a slice of the global basket list
         * delimited by basketStart and basketEnd
         */
        private int basketStart;

        /**
         * Offset of this branch's baskets into the global basket list, the
         * transmitted basket information is a slice of the global basket list
         * delimited by basketStart and basketEnd
         */
        private int basketEnd;

        /**
         * List of the byte offsets of the TKeys corresponding to the baskets
         */
        private long[] basketByteOffsets;

        /**
         * representation of the rangeToBasketIDMap where the index is the value
         * minus basketStart and the value at each index is the range that's
         * represented. This is significantly more space efficient
         */
        private Range<Long>[] rangeToBasketID;
        private TBranch.ArrayDescriptor arrayDesc;
        private String path;
        /**
         * Additional info for compressed TBranches
         */
		private CompressedBasketInfo[] compressedBasketInfo;

        public Range<Long>[] getRangeToBasketID() {
            return rangeToBasketID;
        }

        private static class TrimBasketKey {
            private ImmutableRangeMap<Long, Integer> range;
            private int start;
            private int end;

            public TrimBasketKey(ImmutableRangeMap<Long, Integer> range, int start, int end) {
                this.range = range;
                this.start = start;
                this.end = end;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = start + end;
                result = (prime * result) ^ range.hashCode();
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof TrimBasketKey)) {
                    return false;
                }
                TrimBasketKey other = (TrimBasketKey) obj;
                return ((start == other.start) &&
                        (end == other.end) &&
                        (range.equals(other.range)));
            }
        }

        /**
         * Deduplicate range->basket maps, since many (all?) of them will be same
         * for different branches in a file. Guessing 2000 as a good cache size
         * since that's the upper-bound on the number of branches I'd expect to
         * see in a file.
         */
        private static LoadingCache<TrimBasketKey,
                                    Range<Long>[]> dedupRangeMap =
                                        CacheBuilder.newBuilder()
                                        .maximumSize(2000)
                                        .softValues()
                                        .build(
                                           new CacheLoader<TrimBasketKey,
                                                           Range<Long>[]>() {
                                                @Override
                                                public Range<Long>[] load(TrimBasketKey key) {
                                                    ImmutableMap<Range<Long>, Integer> map = key.range.asMapOfRanges();
                                                    Range<Long>[] rangeToBasketID = new Range[key.end - key.start];
                                                    for (Entry<Range<Long>, Integer> e: map.entrySet()) {
                                                        int idx = e.getValue();
                                                        if ((idx < key.start) || (idx >= key.end)) {
                                                            continue;
                                                        }
                                                        Range<Long> val = e.getKey();
                                                        rangeToBasketID[idx - key.start] = val;
                                                    }
                                                    return rangeToBasketID;
                                                }
                                                });

        protected SerializeStorage(SlimTBranch in) {
            in.checkInvariants();
            path = in.getPath();
            basketStart = in.basketStart;
            basketEnd = in.basketEnd;
            arrayDesc = in.getArrayDesc();

            /*
             * Store the byte offset of each basket
             */
            int compressedCount = 0;
            basketByteOffsets = new long[in.basketEnd - in.basketStart];
            for (int i = in.basketStart; i < in.basketEnd; i += 1) {
                int idx = i - basketStart;
                SlimTBasket basket = in.getBasket(i);
                basketByteOffsets[idx] = basket.getOffset();
                CompressedBasketInfo loc = basket.getCompressedBasketInfo();
                if (loc != null && compressedBasketInfo == null) {
                	if (compressedBasketInfo == null) {
                		compressedBasketInfo = new CompressedBasketInfo[in.basketEnd - in.basketStart];
                	}
                	compressedBasketInfo[idx] = loc;
                }
            }
//            if (compressedCount != 0) {
//                for (int i = in.basketStart; i < in.basketEnd; i += 1) {
//                    int idx = i - basketStart;
//                    SlimTBasket basket = in.getBasket(i);
//                    basketByteOffsets[idx] = basket.getOffset();
//                    if (basket.getCompressedBasketInfo() != null) {
//                    	compressedCount += 1;
//                    }
//                }
//            }

            /*
             * Store the entry range of each basketID
             */
            ImmutableRangeMap<Long, Integer> idmap = in.getRangeToBasketIDMap();
            ImmutableMap<Range<Long>, Integer> map = idmap.asMapOfRanges();
            rangeToBasketID = new Range[basketEnd - basketStart];
            for (Entry<Range<Long>, Integer> e: map.entrySet()) {
                int idx = e.getValue();
                if ((idx < basketStart) || (idx >= basketEnd)) {
                    continue;
                }
                Range<Long> val = e.getKey();
                rangeToBasketID[idx - basketStart] = val;
            }
            TrimBasketKey cacheKey = new TrimBasketKey(in.getRangeToBasketIDMap(), basketStart, basketEnd);
            rangeToBasketID = dedupRangeMap.getUnchecked(cacheKey);
            checkNotNull(rangeToBasketID);
        }

        /**
         * Called by Java deserialization routines
         * @return The SlimTBranch object represented by this object
         * @throws ObjectStreamException We don't throw, but required by Java in signature
         */
        private Object readResolve() throws ObjectStreamException {
            checkNotNull(rangeToBasketID);
            SlimTBranch ret = new SlimTBranch(path, rangeToBasketID, arrayDesc, basketStart, compressedBasketInfo);
            int idx = basketStart;
            for (long off: basketByteOffsets) {
                ret.addBasket(idx, new SlimTBasket(off));
                idx += 1;
            }
            if (compressedBasketInfo != null) {
            	idx = basketStart;
            	for (CompressedBasketInfo i: compressedBasketInfo) {
            		ret.getBasket(idx).setCompressedBasketInfo(i);
            		idx += 1;
            	}
            }
            return ret;
        }

        @Override
        public void validateObject() throws InvalidObjectException {
            if (basketEnd == 0) {
                throw new InvalidObjectException("Null basketEnd");
            }
        }
    }

    /**
     * Called by Java deserialization routines
     * @return The SerializeStorage object which represents this object
     * @throws ObjectStreamException We don't throw, but required by Java in signature
     */
    private Object writeReplace() throws ObjectStreamException {
        return new SerializeStorage(this);
    }

    @Override
    public void validateObject() throws InvalidObjectException {
        if (basketEnd == 0) {
            throw new InvalidObjectException("Got zero-length baskets");
        }
    }

    /*
     * Enable kryo serialization for ImmutableRangeMaps, which apparently
     * doesn't properly serialize with Kryo
     */
    private static class ImmutableRangeMapSerializer extends Serializer<ImmutableRangeMap<Long, Integer>> {
        private static final boolean DOES_NOT_ACCEPT_NULL = true;
        private static final boolean IMMUTABLE = true;

        public ImmutableRangeMapSerializer() {
            super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
        }

        @Override
        public void write(Kryo kryo, Output output, ImmutableRangeMap<Long, Integer> object) {
            kryo.writeObject(output, Maps.newHashMap(object.asMapOfRanges()));
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public ImmutableRangeMap<Long, Integer> read(Kryo kryo, Input input,
                Class<ImmutableRangeMap<Long, Integer>> type) {
            HashMap<Range<Long>, Integer> tmp = new HashMap<Range<Long>, Integer>();
            Class<? extends HashMap> tmpCls = tmp.getClass();
            HashMap<Range<Long>, Integer> hashMapOfRanges = kryo.readObject(input, tmpCls);
            Builder<Long, Integer> builder = new Builder<>();
            for (Entry<Range<Long>, Integer> entry : hashMapOfRanges.entrySet()) {
                builder.put(entry.getKey(), entry.getValue());
            }
            return builder.build();
        }


    }

    /*
     * Implements KryoSerializable interface
     */
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(path);
        output.writeInt(basketEntryOffsetsLength, true);
        output.writeInt(basketStart, true);
        output.writeInt(basketEnd, true);
        kryo.writeObjectOrNull(output, arrayDesc, TBranch.ArrayDescriptor.class);
        kryo.writeObject(output, baskets);
        kryo.writeObject(output, rangeToBasketIDMap, new ImmutableRangeMapSerializer());
    }

    /*
     * Implements KryoSerializable interface
     */
    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        path = input.readString();
        basketEntryOffsetsLength = input.readInt(true);
        basketStart = input.readInt(true);
        basketEnd = input.readInt(true);
        arrayDesc = kryo.readObjectOrNull(input, TBranch.ArrayDescriptor.class);
        baskets = kryo.readObject(input, HashMap.class);
        rangeToBasketIDMap = kryo.readObject(input, ImmutableRangeMap.class, new ImmutableRangeMapSerializer());
    }


}
