package edu.vanderbilt.accre.laurelin.array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public abstract class PrimitiveArray extends Array {
    ByteBuffer disk_buffer;

    PrimitiveArray(Interpretation interpretation, int length) {
        super(interpretation, length);
        this.disk_buffer = ByteBuffer.allocate(length * ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
    }

    PrimitiveArray(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray.length() / ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.disk_buffer = rawarray.raw();
    }

    protected PrimitiveArray(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, (buffer.limit() - buffer.position()) / ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.disk_buffer = buffer;
    }

    public int disk_itemsize() {
        return ((AsDtype)interpretation).disk_itemsize();
    }

    public int memory_itemsize() {
        return ((AsDtype)interpretation).memory_itemsize();
    }

    public int multiplicity() {
        return ((AsDtype)interpretation).multiplicity();
    }

    public int numitems() {
        return this.length * this.multiplicity();
    }

    public void copyitems(PrimitiveArray source, int itemstart, int itemstop) {
        int bytestart = itemstart * this.memory_itemsize();
        int bytestop = itemstop * this.memory_itemsize();
        /*
         * This code takes advantage of the fact that tmp and this.buffer share
         * the same backing array after duplicate()
         */
        ByteBuffer tmp = this.disk_buffer.duplicate();
        tmp.position(bytestart);
        tmp.limit(bytestop);
        ByteBuffer srctmp = source.disk_buffer.duplicate();
        srctmp.position(0);
        tmp.put(srctmp);
    }

    @Override
    public Array clip(int start, int stop) {
        int mult = this.multiplicity();
        int bytestart = start * mult * this.memory_itemsize();
        int bytestop = stop * mult * this.memory_itemsize();
        ByteBuffer out = this.disk_buffer.duplicate();
        out.position(bytestart);
        out.limit(bytestop);
        return this.make(out);
    }

    public RawArray rawarray() {
        return new RawArray(this.disk_buffer);
    }

    protected ByteBuffer raw() {
        return this.disk_buffer;
    }

    protected abstract Array make(ByteBuffer out);

    /////////////////////////////////////////////////////////////////////////// Bool

    /*
     * Note, this is NOT a simple copy-paste of the other numeric types
     */
    public static class Bool extends PrimitiveArray {
        public Bool(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Bool(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Bool(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Bool(int length) {
            super(new AsDtype(AsDtype.Dtype.BOOL), length);
        }

        public Bool(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.BOOL), rawarray);
        }

        protected Bool(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.BOOL), buffer);
        }

        public Bool(byte[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.BOOL), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            ByteBuffer buf = this.disk_buffer.duplicate();
            byte[] out = new byte[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Bool(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Bool(this.interpretation.subarray(), this.disk_buffer);
        }

        public boolean toBoolean(int index) {
            return (this.disk_buffer.get(index) != 0);
        }

        @Override
        public String toString() {
            return Arrays.toString((boolean[])this.toArray());
        }
    }

    /////////////////////////////////////////////////////////////////////////// Int1

    public static class Int1 extends PrimitiveArray {
        public Int1(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Int1(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Int1(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Int1(int length) {
            super(new AsDtype(AsDtype.Dtype.INT1), length);
        }

        public Int1(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT1), rawarray);
        }

        protected Int1(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT1), buffer);
        }

        public Int1(byte[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.INT1), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            //this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            ByteBuffer buf = this.disk_buffer.duplicate();
            byte[] out = new byte[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Int1(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int1(this.interpretation.subarray(), this.disk_buffer);
        }

        public byte toByte(int index) {
            return this.disk_buffer.get(this.disk_buffer.position() + index);
        }

        @Override
        public String toString() {
            return Arrays.toString((byte[])this.toArray());
        }
    }

    /////////////////////////////////////////////////////////////////////////// Int2

    public static class Int2 extends PrimitiveArray {
        public Int2(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Int2(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Int2(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Int2(int length) {
            super(new AsDtype(AsDtype.Dtype.INT2), length);
        }

        public Int2(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT2), rawarray);
        }

        protected Int2(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT2), buffer);
        }

        public Int2(short[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.INT2), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.asShortBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            ShortBuffer buf = this.disk_buffer.asShortBuffer();
            short[] out = new short[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Int2(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int2(this.interpretation.subarray(), this.disk_buffer);
        }

        public short toShort(int index) {
            return this.disk_buffer.asShortBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((short[])this.toArray());
        }
    }

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
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.asIntBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            IntBuffer buf = this.disk_buffer.asIntBuffer();
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
            return new Int4(this.interpretation.subarray(), this.disk_buffer);
        }

        public int toInt(int index) {
            return this.disk_buffer.asIntBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((int[])this.toArray());
        }

        public Int4 add(boolean bigEndian, int value) {
            ByteBuffer out = ByteBuffer.allocate(length * 4);
            out.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            IntBuffer outint = out.asIntBuffer();
            IntBuffer thisint = this.disk_buffer.asIntBuffer();
            for (int i = 0;  i < this.length;  i++) {
                outint.put(i, thisint.get(i) + value);
            }
            return new Int4(out);
        }

        public int get(int i) {
            return this.disk_buffer.getInt(i * 4);
        }

        public void put(int i, int value) {
            this.disk_buffer.putInt(i * 4, value);
        }

    }

    /////////////////////////////////////////////////////////////////////////// Int8

    public static class Int8 extends PrimitiveArray {
        public Int8(Interpretation interpretation, int length) {
            super(interpretation, length);
        }

        public Int8(Interpretation interpretation, RawArray rawarray) {
            super(interpretation, rawarray);
        }

        protected Int8(Interpretation interpretation, ByteBuffer buffer) {
            super(interpretation, buffer);
        }

        public Int8(int length) {
            super(new AsDtype(AsDtype.Dtype.INT8), length);
        }

        public Int8(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT8), rawarray);
        }

        protected Int8(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.INT8), buffer);
        }

        public Int8(long[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.INT8), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.asLongBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            LongBuffer buf = this.disk_buffer.asLongBuffer();
            long[] out = new long[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(ByteBuffer out) {
            return new Int8(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int8(this.interpretation.subarray(), this.disk_buffer);
        }

        public long toLong(int index) {
            return this.disk_buffer.asLongBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((long[])this.toArray());
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
            super(new AsDtype(AsDtype.Dtype.FLOAT4), length);
        }

        public Float4(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.FLOAT4), rawarray);
        }

        protected Float4(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.FLOAT4), buffer);
        }

        public Float4(float[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.FLOAT4), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.asFloatBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            FloatBuffer buf = this.disk_buffer.asFloatBuffer();
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
            return new Float4(this.interpretation.subarray(), this.disk_buffer);
        }

        public float toFloat(int index) {
            return this.disk_buffer.asFloatBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((float[])this.toArray());
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
            super(new AsDtype(AsDtype.Dtype.FLOAT8), length);
        }

        public Float8(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.FLOAT8), rawarray);
        }

        protected Float8(ByteBuffer buffer) {
            super(new AsDtype(AsDtype.Dtype.FLOAT8), buffer);
        }

        public Float8(double[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.FLOAT8), data.length);
            this.disk_buffer = ByteBuffer.allocate(data.length * this.disk_itemsize());
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.disk_buffer.asDoubleBuffer().put(data, 0, data.length);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.disk_buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            DoubleBuffer buf = this.disk_buffer.asDoubleBuffer();
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
            return new Float8(this.interpretation.subarray(), this.disk_buffer);
        }

        public double toDouble(int index) {
            return this.disk_buffer.asDoubleBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((double[])this.toArray());
        }
    }
}

