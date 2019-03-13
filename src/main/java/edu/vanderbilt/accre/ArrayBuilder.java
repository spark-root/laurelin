package edu.vanderbilt.accre;

import java.lang.String;
import java.lang.IllegalArgumentException;
import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.PrimitiveArray;
import edu.vanderbilt.accre.array.RawArray;
import edu.vanderbilt.accre.interpretation.Interpretation;

public class ArrayBuilder {
    Interpretation interpretation;
    long[] basketEntryOffsets;

    public ArrayBuilder(Interpretation interpretation, long[] basketEntryOffsets) {
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
    static public interface GetBasket {
        BasketKey basketkey(int basketid);
        RawArray dataWithoutKey(int basketid);   // length must be fObjlen - fKeylen
    }

    public Array build(GetBasket getbasket, Interpretation interpretation, long entrystart, long entrystop) {
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
            return interpretation.empty();
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
            long numitems = interpretation.numitems(numbytes, (int)numentries);
            totalitems += numitems;
            if (totalitems != (int)totalitems) {
                throw new IllegalArgumentException("number of items requested of ArrayBuilder.build must fit into a 32-bit integer");
            }

            basket_itemoffset[j] = basket_itemoffset[j - 1] + (int)numitems;
            basket_entryoffset[j] = basket_entryoffset[j - 1] + (int)numentries;
        }
            
        Array destination = interpretation.destination((int)totalitems, (int)totalentries);

        // this loop can be parallelized!
        for (int j = 0;  j < basketstop - basketstart;  j++) {
            fill(j, getbasket, interpretation, basketkeys, destination, entrystart, entrystop, basketstart, basketstop, basket_itemoffset, basket_entryoffset);
        }

        Array clipped = interpretation.clip(destination,
                                            basket_itemoffset[0],
                                            basket_itemoffset[basket_itemoffset.length - 1],
                                            basket_entryoffset[0],
                                            basket_entryoffset[basket_entryoffset.length - 1]);

        return interpretation.finalize(clipped);
    }

    private void fill(int j, GetBasket getbasket, Interpretation interpretation, BasketKey[] basketkeys, Array destination, long entrystart, long entrystop, int basketstart, int basketstop, int[] basket_itemoffset, int[] basket_entryoffset) {
        int i = j + basketstart;
        
        int local_entrystart = (int)(entrystart - basket_entryoffset[i]);
        if (local_entrystart < 0) {
            local_entrystart = 0;
        }

        int local_numentries = (int)(basket_entryoffset[i + 1] - basket_entryoffset[i]);
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
