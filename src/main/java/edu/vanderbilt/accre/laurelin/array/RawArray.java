package edu.vanderbilt.accre.laurelin.array;

import java.nio.ByteBuffer;

public class RawArray extends PrimitiveArray {
    RawArray(int length) {
        super(null, length);
        this.buffer = ByteBuffer.allocate(length);
    }

    RawArray(RawArray rawarray) {
        super(null, rawarray.length());
        this.buffer = rawarray.raw();
    }

    public RawArray(ByteBuffer buffer) {
        super(null, buffer.limit());
        this.buffer = buffer;
    }

    @Override
    public int itemsize() {
        return 1;
    }

    @Override
    public int multiplicity() {
        return 1;
    }

    public RawArray slice(int start, int stop) {
        ByteBuffer tmp = buffer.duplicate();
        tmp.position(start);
        tmp.limit(stop);
        return new RawArray(tmp.slice());
    }

    @Override
    public Array clip(int start, int stop) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Object toArray(boolean bigEndian) {
        byte[] out = new byte[this.buffer.limit() - this.buffer.position()];
        this.buffer.get(out);
        return out;
    }

    @Override
    protected Array make(ByteBuffer out) {
        return new RawArray(out);
    }
}
