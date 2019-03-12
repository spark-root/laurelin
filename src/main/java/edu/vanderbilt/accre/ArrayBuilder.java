package edu.vanderbilt.accre;

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
    }

    static public interface Basket {
        RawArray get(int i);
    }







}
