package edu.vanderbilt.accre.array;

import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.PrimitiveArrayInt4;

public class JaggedArray extends Array {
    public JaggedArray(Interpretation interpretation, int length, PrimitiveArrayInt4 offsets, Array content) {
        super(interpretation, length);
    }

    public Array clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public Object toArray(boolean bigEndian) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
