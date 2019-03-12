package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;
import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.PrimitiveArray;
import edu.vanderbilt.accre.array.RawArray;

public class PrimitiveArrayInt4 extends PrimitiveArray<PrimitiveArrayInt4> {
    public PrimitiveArrayInt4(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public PrimitiveArrayInt4(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray);
    }

    protected PrimitiveArrayInt4(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, buffer);
    }

    public PrimitiveArrayInt4 clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
