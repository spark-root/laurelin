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
    long[] basketNumBytes;

    public ArrayBuilder(Interpretation interpretation, long[] basketEntryOffsets, long[] basketNumBytes) {
        this.interpretation = interpretation;
        this.basketEntryOffsets = basketEntryOffsets;
        this.basketNumBytes = basketNumBytes;

        // TODO: check for len(basketEntryOffsets) == len(basketNumBytes) + 1
        // TODO: check for basketEntryOffsets monotonically increasing
        // TODO: check for both being non-negative
    }

    static public interface GetBasket {
        int border(int i);
        RawArray data(int i);
    }

    public Array build(long entrystart, long entrystop) {
        int basketstart = this.basketstart(entrystart);
        int basketstop = this.basketstop(entrystop);

        




        
        throw new UnsupportedOperationException("not yet finished");
    }

    private int basketstart(long entrystart) {
        for (int i = 0;  i < this.basketEntryOffsets.length - 1;  i++) {
            if (this.basketEntryOffsets[i] <= entrystart) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("entrystart %ld not in branch", entrystart));
    }

    private int basketstop(long entrystop) {
        for (int i = this.basketEntryOffsets.length - 1;  i > 0;  i--) {
            if (entrystop <= this.basketEntryOffsets[i]) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("entrystop %ld not in branch", entrystop));
    }



}
