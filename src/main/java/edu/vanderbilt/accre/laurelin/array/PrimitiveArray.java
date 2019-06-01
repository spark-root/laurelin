package edu.vanderbilt.accre.laurelin.array;

import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;

import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public abstract class PrimitiveArray extends Array {
    ByteBuffer buffer;

    PrimitiveArray(Interpretation interpretation, int length) {
        super(interpretation, length);
        this.buffer = ByteBuffer.allocate(length * ((interpretation == null) ? 1 : ((AsDtype)interpretation).itemsize() * ((AsDtype)interpretation).multiplicity()));
    }

    PrimitiveArray(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray.length() / ((interpretation == null) ? 1 : ((AsDtype)interpretation).itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.buffer = rawarray.raw();
    }

    protected PrimitiveArray(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, (buffer.limit() - buffer.position()) / ((interpretation == null) ? 1 : ((AsDtype)interpretation).itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.buffer = buffer;
    }

    public int itemsize() {
        return ((AsDtype)interpretation).itemsize();
    }

    public int multiplicity() {
        return ((AsDtype)interpretation).multiplicity();
    }

    public int numitems() {
        return this.length * this.multiplicity();
    }

    public void copyitems(PrimitiveArray source, int itemstart, int itemstop) {
        int bytestart = itemstart * this.itemsize();
        int bytestop = itemstop * this.itemsize();
        ByteBuffer tmp = this.buffer.duplicate();
        tmp.position(bytestart);
        tmp.limit(bytestop);
        tmp.put(source.raw());
    }

    @Override
    public Array clip(int start, int stop) {
        int mult = this.multiplicity();
        int bytestart = start * mult * this.itemsize();
        int bytestop = stop * mult * this.itemsize();
        ByteBuffer out = this.buffer.duplicate();
        out.position(bytestart);
        out.limit(bytestop);
        return this.make(out);
    }

    public RawArray rawarray() {
        return new RawArray(this.buffer);
    }

    protected ByteBuffer raw() {
        return this.buffer;
    }

    abstract protected Array make(ByteBuffer out);

    /////////////////////////////////////////////////////////////////////////// Int4

    public static class Int4 extends PrimitiveArray {
        public Int4(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Int4(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Int4(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Int4(int length) {
            super(new AsDtype(AsDtype.Dtype.INT4), length);
        }

        public Int4(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT4), rawarray);
        }

        protected Int4(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT4), buffer);
        }

        public Int4(int[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.INT4), data.length);
            this.buffer = ByteBuffer.allocate(data.length * this.itemsize());
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.buffer.asIntBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            IntBuffer buf = this.buffer.asIntBuffer();
            int[] out = new int[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Int4(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int4(this.interpretation.subarray(), this.buffer);
        }

        @Override
        public String toString() {
            return Arrays.toString((int[])this.toArray());
        }

        public Int4 add(boolean bigEndian, int value) {
            ByteBuffer out = ByteBuffer.allocate(length * 4);
            out.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            IntBuffer outint = out.asIntBuffer();
            IntBuffer thisint = this.buffer.asIntBuffer();
            for (int i = 0;  i < this.length;  i++) {
                outint.put(i, thisint.get(i) + value);
            }
            return new Int4(out);
        }

        public int get(int i) {
            return this.buffer.getInt(i * 4);
        }

        public void put(int i, int value) {
            this.buffer.putInt(i * 4, value);
        }

    }

    /////////////////////////////////////////////////////////////////////////// Float8

    public static class Float8 extends PrimitiveArray {
        public Float8(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Float8(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Float8(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Float8(int length) {
            super(new AsDtype(AsDtype.Dtype.INT8), length);
        }

        public Float8(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT8), rawarray);
        }

        protected Float8(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT8), buffer);
        }

        public Float8(double[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.FLOAT8), data.length);
            this.buffer = ByteBuffer.allocate(data.length * this.itemsize());
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.buffer.asDoubleBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            DoubleBuffer buf = this.buffer.asDoubleBuffer();
            double[] out = new double[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Float8(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Float8(this.interpretation.subarray(), this.buffer);
        }

        @Override
        public String toString() {
            return Arrays.toString((double[])this.toArray());
        }
    }

    /////////////////////////////////////////////////////////////////////////// Float4

    public static class Float4 extends PrimitiveArray {
        public Float4(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Float4(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Float4(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Float4(int length) {
            super(new AsDtype(AsDtype.Dtype.INT4), length);
        }

        public Float4(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT4), rawarray);
        }

        protected Float4(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT4), buffer);
        }

        public Float4(float[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.FLOAT4), data.length);
            this.buffer = ByteBuffer.allocate(data.length * this.itemsize());
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.buffer.asFloatBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            FloatBuffer buf = this.buffer.asFloatBuffer();
            float[] out = new float[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Float4(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Float4(this.interpretation.subarray(), this.buffer);
        }

        public float toFloat() {
            return this.buffer.asFloatBuffer().get(0);
        }

        @Override
        public String toString() {
            return Arrays.toString((float[])this.toArray());
        }
    }
}
