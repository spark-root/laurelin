package edu.vanderbilt.accre;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.RawArray;
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
    GetBasket getbasket;
    Interpretation interpretation;
    long[] basketEntryOffsets;

    public ArrayBuilder(GetBasket getbasket, Interpretation interpretation, long[] basketEntryOffsets) {
        this.getbasket = getbasket;
        this.interpretation = interpretation;
        this.basketEntryOffsets = basketEntryOffsets;
        if (basketEntryOffsets.length == 0  ||  basketEntryOffsets[0] != 0) {
            throw new IllegalArgumentException("basketEntryOffsets must start with zero");
        }
        for (int i = 1;  i < basketEntryOffsets.length;  i++) {
            if (basketEntryOffsets[i] < basketEntryOffsets[i - 1]) {
                throw new IllegalArgumentException("basketEntryOffsets must be monotonically increasing");
            }
        }
    }

    public Array build(long entrystart, long entrystop) {
        int basketstart = -1;
        int basketstop = -1;

        for (int i = 0;  i < this.basketEntryOffsets.length - 1;  i++) {
            if (basketstart == -1) {
                if (entrystart < this.basketEntryOffsets[i + 1]  &&  this.basketEntryOffsets[i] < entrystop) {
                    basketstart = i;
                    basketstop = i;
                }
            }
            else {
                if (this.basketEntryOffsets[i] < entrystop) {
                    basketstop = i;
                }
            }
        }

        if (basketstop != -1) {
            basketstop += 1;
        }

        if (basketstart == -1) {
            return this.interpretation.empty();
        }

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
            long numentries = this.basketEntryOffsets[j] - this.basketEntryOffsets[j - 1];
            totalentries += numentries;
            if (totalentries != (int)totalentries) {
                throw new IllegalArgumentException("number of entries requested of ArrayBuilder.build must fit into a 32-bit integer");
            }

            int numbytes = basketkeys[j - 1].fLast - basketkeys[j - 1].fKeylen;
            long numitems = this.interpretation.numitems(numbytes, (int)numentries);
            totalitems += numitems;
            if (totalitems != (int)totalitems) {
                throw new IllegalArgumentException("number of items requested of ArrayBuilder.build must fit into a 32-bit integer");
            }

            basket_itemoffset[j] = basket_itemoffset[j - 1] + (int)numitems;
            basket_entryoffset[j] = basket_entryoffset[j - 1] + (int)numentries;
        }

        Array destination = this.interpretation.destination((int)totalitems, (int)totalentries);

        // This loop can be parallelized: instances change *different parts of* destination, basket_itemoffset, basket_entryoffset.
        for (int j = 0;  j < basketstop - basketstart;  j++) {
            fill(j, basketkeys, destination, entrystart, entrystop, basketstart, basketstop, basket_itemoffset, basket_entryoffset);
        }

        Array clipped = this.interpretation.clip(destination,
                basket_itemoffset[0],
                basket_itemoffset[basket_itemoffset.length - 1],
                basket_entryoffset[0],
                basket_entryoffset[basket_entryoffset.length - 1]);

        return this.interpretation.finalize(clipped);
    }

    private void fill(int j, BasketKey[] basketkeys, Array destination, long entrystart, long entrystop, int basketstart, int basketstop, int[] basket_itemoffset, int[] basket_entryoffset) {
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
            source = this.interpretation.fromroot(basketdata, null, local_entrystart, local_entrystop);
        }
        else {
            // get byteoffsets from basketdata for jagged arrays
            throw new UnsupportedOperationException("not done yet");
        }

        int expecteditems = basket_itemoffset[j + 1] - basket_itemoffset[j];
        int source_numitems = this.interpretation.source_numitems(source);

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

        this.interpretation.fill(source,
                destination,
                basket_itemoffset[j],
                basket_itemoffset[j + 1],
                basket_entryoffset[j],
                basket_entryoffset[j + 1]);
    }
}
