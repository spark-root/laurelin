package edu.vanderbilt.accre.laurelin.array;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class ArrayBuilder {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Subset of only the values in TKey which describes the basket on-disk.
     *
     */
    public static class BasketKey {
        int fKeylen;
        int fLast;
        int fObjlen;

        public BasketKey(int fKeylen, int fLast, int fObjlen) {
            this.fKeylen = fKeylen;
            this.fLast = fLast;
            this.fObjlen = fObjlen;
        }
    }

    /**
     * Callback interface used by the array interface to request additional info
     * about baskets from the root_proxy layer
     */
    public static interface GetBasket {
        /**
         * Get the BasketKey describing a certain basketid.
         * @param basketid the zero-indexed basket index for the given branch
         * @return BasketKey filled with info about the chosen basket
         */
        public BasketKey basketkey(int basketid);

        /**
         * Retrieves the decompressed bytes within the basket, excluding the
         * TKey header.
         * @param basketid the zero-indexed basket index for the given branch
         * @return a RawArray with the decompressed bytes
         */
        public RawArray dataWithoutKey(int basketid);   // length must be fObjlen - fKeylen
    }

    /**
     * Callbacks to get info about a basket from root_proxy.
     */
    Interpretation interpretation;
    int[] basket_itemoffset;
    int[] basket_entryoffset;
    BasketKey[] basketkeys;
    private Array array;
    Array output_relative;
    Array output_whole;
    ArrayList<FutureTask<Boolean>> tasks;
    private long[] basketEntryOffsets;
    int global_offset_whole;

    private Array processBasket(long entryOffset, long itemOffset, Range<Long> entryRange, int basketId, GetBasket basketCallback, Array output) {
        // Put entryRange from the given basketId into output, starting at entry/itemoffset in the destination
        int entryStart = Math.toIntExact(entryRange.lowerEndpoint());
        int entryStop = Math.toIntExact(entryRange.upperEndpoint());
        int entries = entryStop - entryStart;
        BasketKey basketKey = basketCallback.basketkey(basketId);;
        int bytes = basketKey.fLast - basketKey.fKeylen;
        int items = interpretation.numitems(bytes, entries);
        logger.trace("e0: {} e1: {} i0: {} i1: {} estart: {} estop: {} b: {}", entryOffset, entries, itemOffset, items, entryStart, entryStop);

        RawArray basketdata = basketCallback.dataWithoutKey(basketId);
        Array source = null;


        int border = basketKey.fLast - basketKey.fKeylen;
        if (basketKey.fObjlen == border) {
            basketdata = interpretation.convertBufferDiskToMemory(basketdata);
            source = interpretation.fromroot(basketdata, null, 0, entryStop - entryStart);
        } else {
            RawArray content = basketdata.slice(0, border);
            RawArray offsets = basketdata.slice(border + 4, basketKey.fObjlen);
            PrimitiveArray.Int4 testcontent = new PrimitiveArray.Int4(content);
            PrimitiveArray.Int4 testoffsets = new PrimitiveArray.Int4(offsets);
            PrimitiveArray.Int4 byteoffsets = new PrimitiveArray.Int4(offsets).add(true, -basketKey.fKeylen);
            byteoffsets.put(byteoffsets.length() - 1, border);
            content = interpretation.subarray().convertBufferDiskToMemory(content);
            byteoffsets = interpretation.subarray().convertOffsetDiskToMemory(byteoffsets);
            int start = (0);
            int stop = (entryStop - entryStart);
            //System.out.println(String.format("pp1 start: %d stop: %d content: %s byteoffsets: %s", start, stop, testcontent, byteoffsets));
            source = interpretation.fromroot(content, byteoffsets, start, stop);
        }

        interpretation.fill(source,
                output,
                (int) itemOffset,
                (int) (itemOffset + items),
                (int) entryOffset,
                (int) (entryOffset + entries));
        return output;
    }

    public ArrayBuilder(GetBasket getbasket, Interpretation interpretation, long[] basketEntryOffsets, Executor executor, long entrystart, long entrystop) {
        this.basketEntryOffsets = basketEntryOffsets;
        this.interpretation = interpretation;
        this.global_offset_whole = -1;

        if (basketEntryOffsets.length == 0  ||  basketEntryOffsets[0] != 0) {
            throw new IllegalArgumentException("basketEntryOffsets must start with zero");
        }
        for (int i = 1;  i < basketEntryOffsets.length;  i++) {
            if (basketEntryOffsets[i] < basketEntryOffsets[i - 1]) {
                throw new IllegalArgumentException("basketEntryOffsets must be monotonically increasing "
                                + Integer.toString(i) + " / " + Integer.toString(basketEntryOffsets.length)
                                + ": "  + Long.toString(basketEntryOffsets[i])
                                + " ?>? " + Long.toString(basketEntryOffsets[i - 1])
                                + " offsets: " + Arrays.toString(basketEntryOffsets));
            }
        }
        Builder<Long, Integer> rangeBuilder = ImmutableRangeMap.builder();
        for (int i = 1; i < basketEntryOffsets.length; i += 1) {
            rangeBuilder = rangeBuilder.put(Range.closedOpen(basketEntryOffsets[i - 1], basketEntryOffsets[i]), i - 1);
        }
        rangeBuilder = rangeBuilder.put(Range.atLeast(basketEntryOffsets[basketEntryOffsets.length - 1]), basketEntryOffsets.length - 1);
        ImmutableRangeMap<Long, Integer> entryRangeMap = rangeBuilder.build();

        ImmutableRangeMap<Long, Integer> intersection = entryRangeMap.subRangeMap(Range.closedOpen(entrystart, entrystop));
        // This emits the entryRange and associated basketid we need to process
        ImmutableSet<Entry<Range<Long>, Integer>> intersectionEntries = intersection.asMapOfRanges().entrySet();
        long entryOffset_whole = 0;
        long itemOffset_whole = 0;

        // Loop once to calculate the length of the output buffer
        for (Entry<Range<Long>, Integer> entry: intersectionEntries) {
            Range<Long> entryRange = entry.getKey();
            Integer basketId = entry.getValue();
            BasketKey key = getbasket.basketkey(basketId);
            int bytes = key.fLast - key.fKeylen;

            // whole basket
            long entries_whole = basketEntryOffsets[basketId + 1] - basketEntryOffsets[basketId];
            Range<Long> entryRange_whole = Range.closedOpen(basketEntryOffsets[basketId], basketEntryOffsets[basketId + 1]);
            long items_whole = interpretation.numitems(bytes, (int)entries_whole);

            // postlogue
            entryOffset_whole += entries_whole;
            itemOffset_whole += items_whole;
        }
        //System.out.println("making dest " + itemOffset_whole + " " + entryOffset_whole);
        output_whole = interpretation.destination((int)itemOffset_whole, (int)entryOffset_whole);
        entryOffset_whole = 0;
        itemOffset_whole = 0;
        // Now loop again to do the actual filling
        for (Entry<Range<Long>, Integer> entry: intersectionEntries) {
            Range<Long> entryRange = entry.getKey();
            Integer basketId = entry.getValue();
            BasketKey key = getbasket.basketkey(basketId);
            int bytes = key.fLast - key.fKeylen;


            // whole basket
            long entries_whole = basketEntryOffsets[basketId + 1] - basketEntryOffsets[basketId];
            Range<Long> entryRange_whole = Range.closedOpen(basketEntryOffsets[basketId], basketEntryOffsets[basketId + 1]);
            long items_whole = interpretation.numitems(bytes, (int)entries_whole);

            if (global_offset_whole == -1) {
                global_offset_whole = (int) (entrystart - basketEntryOffsets[basketId]);
                //System.out.println("Offset whole is " + global_offset_whole);
            }
            processBasket(entryOffset_whole, itemOffset_whole, entryRange_whole, basketId, getbasket, output_whole);

            // postlogue
            entryOffset_whole += entries_whole;
            itemOffset_whole += items_whole;
        }
//      tasks = new ArrayList<FutureTask<Boolean>>(basketstop - basketstart);
//      if (executor == null) {
//      //fill.call();
//  }
//  else {
//      FutureTask<Boolean> task = new FutureTask<Boolean>(fill);
//      tasks.add(task);
//      executor.execute(task);
//  }
    }

    public Array getArray(int rowId, int count) {
//        for (FutureTask<Boolean> task : tasks) {
//            try {
//                task.get();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e.toString());
//            } catch (ExecutionException e) {
//                throw new RuntimeException(e.toString());
//            }
//        }
        Array x = output_whole.clip(global_offset_whole + rowId, global_offset_whole + rowId + count);
        // Array y = array.clip(basket_entryoffset[0] + rowId, basket_entryoffset[0] + rowId + count);
        return x;
    }
}
