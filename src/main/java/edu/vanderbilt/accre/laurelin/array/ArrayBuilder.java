package edu.vanderbilt.accre.laurelin.array;

import java.util.concurrent.Executor;
import java.lang.Runnable;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class ArrayBuilder {

    /**
     * Subset of only the values in TKey which describes the basket on-disk
     *
     */
    static public class BasketKey {
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
    static public interface GetBasket {
        /**
         * Get the BasketKey describing a certain basketid
         * @param basketid the zero-indexed basket index for the given branch
         * @return BasketKey filled with info about the chosen basket
         */
        public BasketKey basketkey(int basketid);

        /**
         * Retrieves the decompressed bytes within the basket, excluding the
         * TKey header
         * @param basketid the zero-indexed basket index for the given branch
         * @return a RawArray with the decompressed bytes
         */
        public RawArray dataWithoutKey(int basketid);   // length must be fObjlen - fKeylen
    }

    /**
     * Callbacks to get info about a basket from root_proxy
     */
    Array array;

    public Array get() {
        return array;
    }

    public ArrayBuilder(GetBasket getbasket, Interpretation interpretation, long[] basketEntryOffsets, long entrystart, long entrystop) {
        if (basketEntryOffsets.length == 0  ||  basketEntryOffsets[0] != 0) {
            throw new IllegalArgumentException("basketEntryOffsets must start with zero");
        }
        for (int i = 1;  i < basketEntryOffsets.length;  i++) {
            if (basketEntryOffsets[i] < basketEntryOffsets[i - 1]) {
                throw new IllegalArgumentException("basketEntryOffsets must be monotonically increasing");
            }
        }

        int basketstart = -1;
        int basketstop = -1;

        for (int i = 0;  i < basketEntryOffsets.length - 1;  i++) {
            if (basketstart == -1) {
                if (entrystart < basketEntryOffsets[i + 1]  &&  basketEntryOffsets[i] < entrystop) {
                    basketstart = i;
                    basketstop = i;
                }
            }
            else {
                if (basketEntryOffsets[i] < entrystop) {
                    basketstop = i;
                }
            }
        }

        if (basketstop != -1) {
            basketstop += 1;
        }

        if (basketstart == -1) {
            array = interpretation.empty();
        }
        else {
            BasketKey[] basketkeys = new BasketKey[basketstop - basketstart];
            for (int j = 0;  j < basketstop - basketstart;  j++) {
                basketkeys[j] = getbasket.basketkey(basketstart + j);
            }

            long totalitems = 0;
            long totalentries = 0;
            int[] basket_itemoffset = new int[1 + basketstop - basketstart];
            int[] basket_entryoffset = new int[1 + basketstop - basketstart];

            basket_itemoffset[0] = 0;
            basket_entryoffset[0] = 0;
            for (int j = 1;  j < 1 + basketstop - basketstart;  j++) {
                long numentries = basketEntryOffsets[j] - basketEntryOffsets[j - 1];
                totalentries += numentries;
                if (totalentries != (int)totalentries) {
                    throw new IllegalArgumentException("number of entries requested of ArrayBuilder.build must fit into a 32-bit integer");
                }

                int numbytes = basketkeys[j - 1].fLast - basketkeys[j - 1].fKeylen;
                long numitems = interpretation.numitems(numbytes, (int)numentries);
                totalitems += numitems;
                if (totalitems != (int)totalitems) {
                    throw new IllegalArgumentException("number of items requested of ArrayBuilder.build must fit into a 32-bit integer");
                }

                basket_itemoffset[j] = basket_itemoffset[j - 1] + (int)numitems;
                basket_entryoffset[j] = basket_entryoffset[j - 1] + (int)numentries;
            }

            Array destination = interpretation.destination((int)totalitems, (int)totalentries);

            // This loop can be parallelized: instances change *different parts of* destination, basket_itemoffset, basket_entryoffset.
            for (int j = 0;  j < basketstop - basketstart;  j++) {
                Runnable runnable = new FillRunnable(interpretation, getbasket, j, basketkeys, destination, entrystart, entrystop, basketstart, basketstop, basket_itemoffset, basket_entryoffset);
                runnable.run();
            }

            Array clipped = interpretation.clip(destination,
                                                basket_itemoffset[0],
                                                basket_itemoffset[basket_itemoffset.length - 1],
                                                basket_entryoffset[0],
                                                basket_entryoffset[basket_entryoffset.length - 1]);

            array = interpretation.finalize(clipped);
        }
    }

    static private class FillRunnable implements Runnable {
        Interpretation interpretation;
        GetBasket getbasket;
        int j;
        BasketKey[] basketkeys;
        Array destination;
        long entrystart;
        long entrystop;
        int basketstart;
        int basketstop;
        int[] basket_itemoffset;
        int[] basket_entryoffset;

        FillRunnable(Interpretation interpretation, GetBasket getbasket, int j, BasketKey[] basketkeys, Array destination, long entrystart, long entrystop, int basketstart, int basketstop, int[] basket_itemoffset, int[] basket_entryoffset) {
            this.interpretation = interpretation;
            this.getbasket = getbasket;
            this.j = j;
            this.basketkeys = basketkeys;
            this.destination = destination;
            this.entrystart = entrystart;
            this.entrystop = entrystop;
            this.basketstart = basketstart;
            this.basketstop = basketstop;
            this.basket_itemoffset = basket_itemoffset;
            this.basket_entryoffset = basket_entryoffset;
        }

        public void run() {
            int i = j + basketstart;

            int local_entrystart = (int)(entrystart - basket_entryoffset[i]);
            if (local_entrystart < 0) {
                local_entrystart = 0;
            }

            int local_numentries = basket_entryoffset[i + 1] - basket_entryoffset[i];
            int local_entrystop = (int)(entrystop - basket_entryoffset[i]);
            if (local_entrystop > local_numentries) {
                local_entrystop = local_numentries;
            }
            if (local_entrystop < 0) {
                local_entrystop = 0;
            }

            RawArray basketdata = getbasket.dataWithoutKey(i);
            Array source = null;
            if (basketkeys[i].fObjlen == basketkeys[i].fLast - basketkeys[i].fKeylen) {
                source = interpretation.fromroot(basketdata, null, local_entrystart, local_entrystop);
            }
            else {
                // get byteoffsets from basketdata for jagged arrays
                throw new UnsupportedOperationException("not done yet");
            }

            int expecteditems = basket_itemoffset[j + 1] - basket_itemoffset[j];
            int source_numitems = interpretation.source_numitems(source);

            int expectedentries = basket_entryoffset[j + 1] - basket_entryoffset[j];
            int source_numentries = local_entrystop - local_entrystart;

            if (j + 1 == basketstop - basketstart) {
                if (expecteditems > source_numitems) {
                    basket_itemoffset[j + 1] -= expecteditems - source_numitems;
                }
                if (expectedentries > source_numentries) {
                    basket_entryoffset[j + 1] -= expectedentries - source_numentries;
                }
            }
            else if (j == 0) {
                if (expecteditems > source_numitems) {
                    basket_itemoffset[j] += expecteditems - source_numitems;
                }
                if (expectedentries > source_numentries) {
                    basket_entryoffset[j] += expectedentries - source_numentries;
                }
            }

            interpretation.fill(source,
                                destination,
                                basket_itemoffset[j],
                                basket_itemoffset[j + 1],
                                basket_entryoffset[j],
                                basket_entryoffset[j + 1]);
        }
    }
}