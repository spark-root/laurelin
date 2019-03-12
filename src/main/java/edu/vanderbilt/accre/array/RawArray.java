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

    public RawArray byteswap(int itemsize) {
        if (itemsize <= 1) {
            return this;
        }
        byte[] buf = new byte[itemsize];
        for (int i = 0;  i < this.length;  i += itemsize) {
            this.buffer.get(buf, i, itemsize);
            for (int j = 0;  j < itemsize / 2;  j++) {
                byte tmp = buf[j];
                buf[j] = buf[itemsize - j - 1];
                buf[itemsize - j - 1] = tmp;
            }
            this.buffer.put(buf, i, itemsize);
        }
        return this;
    }

    protected ByteBuffer raw() {
        return this.buffer;
    }
}
