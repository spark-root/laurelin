package edu.vanderbilt.accre.laurelin.array;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public abstract class Array {
    Interpretation interpretation = null;
    int length;

    Array(Interpretation interpretation, int length) {
        this.interpretation = interpretation;
        this.length = length;
    }

    public Interpretation interpretation() {
        return this.interpretation;
    }

    public int length() {
        return this.length;
    }

    public abstract Array clip(int start, int stop);

    public Object toArray() {
        return this.toArray(true);
    }

    public abstract Object toArray(boolean bigEndian);

    public abstract Array subarray();
}
