package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.interpretation.Interpretation;

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

    abstract public Array clip(int start, int stop);

    public Object toArray() {
        return this.toArray(true);
    }
    abstract public Object toArray(boolean bigEndian);
}
