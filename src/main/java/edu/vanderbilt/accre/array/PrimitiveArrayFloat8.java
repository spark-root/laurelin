package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class PrimitiveArrayFloat8 extends PrimitiveArray<PrimitiveArrayFloat8> {
    public PrimitiveArrayFloat8(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public PrimitiveArrayFloat8(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray);
    }

    protected PrimitiveArrayFloat8(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, buffer);
    }

    public PrimitiveArrayFloat8 clip(int start, int stop) {
        return new PrimitiveArrayFloat8(this.interpretation, this.rawclipped(start, stop));
    }
}
