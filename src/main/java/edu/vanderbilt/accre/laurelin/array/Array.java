package edu.vanderbilt.accre.laurelin.array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

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

    public static LaurelinBackingArray wrap(ByteBuffer buf) {
        return NIOBuf.wrap(buf);
    }

    /**
     * Interface encapsulating ByteBuffer functionality, which lets us
     * swap in Arrow buffers instead if we want
     *
     */
    public static interface LaurelinBackingArray {
        public boolean isDirect();

        public LaurelinBackingArray duplicate();

        public void position(int bytestart);

        public void limit(int bytestop);

        public void put(LaurelinBackingArray srctmp);

        public LaurelinBackingArray slice();

        public void order(ByteOrder bo);

        public DoubleBuffer asDoubleBuffer();

        public FloatBuffer asFloatBuffer();

        public LongBuffer asLongBuffer();

        public IntBuffer asIntBuffer();

        public int getInt(int i);

        public void putInt(int i, int value);

        public static LaurelinBackingArray allocate(int i) {
            return NIOBuf.wrap(ByteBuffer.allocate(i));
        }

        public ShortBuffer asShortBuffer();

        /**
         *
         * @return position of the buffer in BYTES
         */
        public int position();

        public byte get(int i);

        public void get(byte[] out);

        public int limit();

        public void put(byte[] copy);

        public void get(byte[] ret, int i, int j);

        public long getLong(int i);

        public void putInt(int last);

        public void flip();

        public void put(int i, byte byte1);

        public void put(byte byte1);

    }

    public static final class NIOBuf implements LaurelinBackingArray {
        ByteBuffer buf;
        public ByteBuffer toByteBuffer() {
            ByteBuffer ret = buf.duplicate();
            ret.position(0);
            return ret.slice();
        }

        private NIOBuf(ByteBuffer duplicate) {
            this.buf = duplicate;
        }

        @Override
        public boolean isDirect() {
            return !buf.isDirect();
        }

        @Override
        public LaurelinBackingArray duplicate() {
            return NIOBuf.wrap(buf.duplicate());
        }

        private static LaurelinBackingArray wrap(ByteBuffer duplicate) {
            return new NIOBuf(duplicate);
        }

        @Override
        public void position(int bytestart) {
            buf.position(bytestart);
        }

        @Override
        public void limit(int bytestop) {
            buf.limit(bytestop);
        }

        @Override
        public void put(LaurelinBackingArray srctmp) {
            buf.put(((NIOBuf )srctmp).buf);
        }

        @Override
        public LaurelinBackingArray slice() {
            return NIOBuf.wrap(buf.slice());
        }

        @Override
        public void order(ByteOrder bo) {
            buf.order(bo);
        }

        @Override
        public DoubleBuffer asDoubleBuffer() {
            return buf.asDoubleBuffer();
        }

        @Override
        public FloatBuffer asFloatBuffer() {
            return buf.asFloatBuffer();
        }

        @Override
        public LongBuffer asLongBuffer() {
            return buf.asLongBuffer();
        }

        @Override
        public IntBuffer asIntBuffer() {
            return buf.asIntBuffer();
        }

        @Override
        public int getInt(int i) {
            return buf.getInt(i);
        }

        @Override
        public void putInt(int i, int value) {
            buf.putInt(i, value);
        }

        @Override
        public ShortBuffer asShortBuffer() {
            return buf.asShortBuffer();
        }

        @Override
        public int position() {
            return buf.position();
        }

        @Override
        public byte get(int i) {
            return buf.get(i);
        }

        @Override
        public void get(byte[] out) {
            buf.get(out);
        }

        @Override
        public int limit() {
            return buf.limit();
        }

        @Override
        public void put(byte[] copy) {
            buf.put(copy);
        }

        @Override
        public void get(byte[] ret, int i, int j) {
            buf.get(ret, i, j);
        }

        @Override
        public long getLong(int i) {
            return buf.getLong(i);
        }

        @Override
        public void putInt(int last) {
            buf.putInt(last);
        }

        @Override
        public void flip() {
            buf.flip();
        }

        @Override
        public void put(int i, byte byte1) {
            buf.put(i, byte1);

        }

        @Override
        public void put(byte byte1) {
            buf.put(byte1);
        }
    }
}
