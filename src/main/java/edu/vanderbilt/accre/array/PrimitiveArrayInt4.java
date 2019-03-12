package edu.vanderbilt.accre.array;

import java.nio.IntBuffer;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class PrimitiveArrayInt4 extends PrimitiveArray {
    IntBuffer buffer;

    public PrimitiveArrayInt4(Interpretation interpretation, int length) {
        super(interpretation, length);
        this.buffer = IntBuffer.allocate(this.numitems());
    }
}
