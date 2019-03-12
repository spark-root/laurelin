package edu.vanderbilt.accre.array;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class PrimitiveArrayFloat8 extends PrimitiveArray<PrimitiveArrayFloat8> {
    public PrimitiveArrayFloat8(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public PrimitiveArrayFloat8(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray);
    }

    public PrimitiveArrayFloat8 clip(int start, int stop) {
        return this;
    }
}
