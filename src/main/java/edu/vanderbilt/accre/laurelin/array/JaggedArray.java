package edu.vanderbilt.accre.laurelin.array;

import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class JaggedArray extends Array {
    PrimitiveArray.Int4 offsets;
    Array content;

    public JaggedArray(Interpretation interpretation, int length, PrimitiveArray.Int4 offsets, Array content) {
        super(interpretation, length);
        this.offsets = offsets;
        this.content = content;
    }

    public PrimitiveArray.Int4 offsets() {
        return this.offsets;
    }

    public Array content() {
        return this.content;
    }

    public Array clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public Object toArray(boolean bigEndian) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Array subarray() {
        throw new UnsupportedOperationException("not implemented yet");
    }

}
