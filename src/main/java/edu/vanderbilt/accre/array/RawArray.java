package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.array.Array;

public class RawArray extends Array {
    ByteBuffer buffer;

    public RawArray(int length) {
        super(null, length);
        this.buffer = ByteBuffer.allocate(length);
    }

    public RawArray(RawArray rawarray) {
        super(null, rawarray.length());
        this.buffer = rawarray.raw().duplicate();
    }

    protected RawArray(ByteBuffer buffer) {
        super(null, buffer.limit());
        this.buffer = buffer;
    }

    public RawArray slice(int start, int stop) {
        ByteBuffer tmp = buffer.duplicate();
        tmp.position(start);
        tmp.limit(stop);
        return new RawArray(tmp.slice());
    }

    protected ByteBuffer raw() {
        return this.buffer;
    }
}
