package edu.vanderbilt.accre.array;

import java.lang.Integer;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.interpretation.AsDtype;
import edu.vanderbilt.accre.array.Array;

public abstract class PrimitiveArray extends Array {
    PrimitiveArray(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public int numitems() {
        int out = this.length;
        for (Integer i : ((AsDtype)this.interpretation).dims()) {
            out *= i;
        }
        return out;
    }
}
