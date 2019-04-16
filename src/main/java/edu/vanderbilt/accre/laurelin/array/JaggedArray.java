package edu.vanderbilt.accre.laurelin.array;

import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class JaggedArray extends Array {
    public JaggedArray(Interpretation interpretation, int length, PrimitiveArray.Int4 offsets, Array content) {
        super(interpretation, length);
    }

    public Array clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public Object toArray(boolean bigEndian) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
