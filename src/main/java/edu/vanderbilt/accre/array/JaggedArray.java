package edu.vanderbilt.accre.array;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.PrimitiveArrayInt4;

public class JaggedArray extends Array {
    public JaggedArray(Interpretation interpretation, int length, PrimitiveArrayInt4 offsets, Array content) {
        super(interpretation, length);
    }
}
