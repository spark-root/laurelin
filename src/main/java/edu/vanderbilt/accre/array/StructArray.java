package edu.vanderbilt.accre.array;

import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.array.Array;

public class StructArray extends Array<StructArray> {
    public StructArray(Interpretation interpretation, int length) {
        super(interpretation, length);
    }
    
    public StructArray clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
