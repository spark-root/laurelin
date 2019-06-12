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
        ByteBuffer out = this.buffer.duplicate();
        out.position(start);
        out.limit(stop);
        return this.make(out);
    }

    public RawArray compact(PrimitiveArray.Int4 byteoffsets, int skipbytes, int local_entrystart, int local_entrystop) {
        if (skipbytes == 0) {
            ByteBuffer out = this.buffer.duplicate();
            out.position(byteoffsets.get(local_entrystart));
            out.limit(byteoffsets.get(local_entrystop));
            return new RawArray(out);
        } else {
            ByteBuffer out = ByteBuffer.allocate(byteoffsets.get(local_entrystop) - byteoffsets.get(local_entrystart) - skipbytes * (local_entrystop - local_entrystart));
            this.buffer.position(0);
            for (int i = local_entrystart;  i < local_entrystop;  i++) {
                int start = byteoffsets.get(i) + skipbytes;
                int count = byteoffsets.get(i + 1) - start;
                byte[] copy = new byte[count];
                this.buffer.get(copy);
                out.put(copy);
            }
            this.buffer.position(0);
            out.position(0);
            return new RawArray(out);
        }
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

    @Override
    public Array subarray() {
        throw new UnsupportedOperationException("RawArray is not subarrayable");
    }
}
