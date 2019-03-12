package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.interpretation.Interpretation;

public abstract class Array<THIS> {
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

    abstract public THIS clip(int start, int stop);
}
