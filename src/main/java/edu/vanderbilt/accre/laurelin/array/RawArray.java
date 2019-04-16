package edu.vanderbilt.accre.laurelin.array;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;

import java.lang.UnsupportedOperationException;

public class RawArray extends PrimitiveArray {
    RawArray(int length) {
        super(null, length);
        this.buffer = ByteBuffer.allocate(length);
    }

    RawArray(RawArray rawarray) {
        super(null, rawarray.length());
        this.buffer = rawarray.raw();
    }

    protected RawArray(ByteBuffer buffer) {
        super(null, buffer.limit());
        this.buffer = buffer;
    }

    public int itemsize() {
        return 1;
    }

    public int multiplicity() {
        return 1;
    }

    public RawArray slice(int start, int stop) {
        ByteBuffer tmp = buffer.duplicate();
        tmp.position(start);
        tmp.limit(stop);
        return new RawArray(tmp.slice());
    }

    public Array clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public Object toArray(boolean bigEndian) {
        byte[] out = new byte[this.buffer.limit() - this.buffer.position()];
        this.buffer.get(out);
        return out;
    }
    
    protected Array make(ByteBuffer out) {
        return new RawArray(out);
    }
}
