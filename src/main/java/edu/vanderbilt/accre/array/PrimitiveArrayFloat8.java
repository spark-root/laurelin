package edu.vanderbilt.accre.array;

import java.nio.DoubleBuffer;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class PrimitiveArrayFloat8 extends PrimitiveArray {
    DoubleBuffer buffer;

    public PrimitiveArrayFloat8(Interpretation interpretation, int length) {
        super(interpretation, length);
        this.buffer = DoubleBuffer.allocate(this.numitems());
    }
}
