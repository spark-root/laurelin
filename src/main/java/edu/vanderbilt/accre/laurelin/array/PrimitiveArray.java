package edu.vanderbilt.accre.laurelin.array;

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
    LaurelinBackingArray buffer;

    PrimitiveArray(Interpretation interpretation, int length) {
        super(interpretation, length);
        this.buffer = LaurelinBackingArray.allocate(length * ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
    }

    PrimitiveArray(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray.length() / ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.buffer = rawarray.raw();
    }

    protected PrimitiveArray(Interpretation interpretation, LaurelinBackingArray buffer) {
        super(interpretation, (buffer.limit() - buffer.position()) / ((interpretation == null) ? 1 : ((AsDtype)interpretation).memory_itemsize() * ((AsDtype)interpretation).multiplicity()));
        this.buffer = buffer;
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
        LaurelinBackingArray tmp = this.buffer.duplicate();
        tmp.position(bytestart);
        tmp.limit(bytestop);
        LaurelinBackingArray srctmp = source.buffer.duplicate();
        srctmp.position(0);
        tmp.put(srctmp);
    }

    @Override
    public Array clip(int start, int stop) {
        int mult = this.multiplicity();
        int bytestart = start * mult * this.memory_itemsize();
        int bytestop = stop * mult * this.memory_itemsize();
        LaurelinBackingArray out = this.buffer.duplicate();
        out.position(bytestart);
        out.limit(bytestop);
        return this.make(out.slice());
    }

    public RawArray rawarray() {
        return new RawArray(this.buffer);
    }

    public LaurelinBackingArray raw() {
        return this.buffer;
    }

    protected abstract Array make(LaurelinBackingArray out);

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

        protected Bool(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            LaurelinBackingArray buf = this.buffer.duplicate();
            byte[] out = new byte[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(LaurelinBackingArray out) {
            return new Bool(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Bool(this.interpretation.subarray(), this.buffer);
        }

        public boolean toBoolean(int index) {
            return (this.buffer.get(this.buffer.position() + index) != 0);
        }

        @Override
        public String toString() {
            Object val = this.toArray();

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

        protected Int1(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            LaurelinBackingArray buf = this.buffer.duplicate();
            byte[] out = new byte[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(LaurelinBackingArray out) {
            return new Int1(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int1(this.interpretation.subarray(), this.buffer);
        }

        public byte toByte(int index) {
            return this.buffer.get(this.buffer.position() + index);
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

        protected Int2(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            ShortBuffer buf = this.buffer.asShortBuffer();
            short[] out = new short[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(LaurelinBackingArray out) {
            return new Int2(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int2(this.interpretation.subarray(), this.buffer);
        }

        public short toShort(int index) {
            return this.buffer.asShortBuffer().get(index);
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

        protected Int4(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
        }

        public Int4(int length) {
            super(new AsDtype(AsDtype.Dtype.INT4), length);
        }

        public Int4(RawArray rawarray) {
            super(new AsDtype(AsDtype.Dtype.INT4), rawarray);
        }

        public Int4(LaurelinBackingArray buffer) {
            super(new AsDtype(AsDtype.Dtype.INT4), buffer);
        }

        public Int4(int[] data, boolean bigEndian) {
            super(new AsDtype(AsDtype.Dtype.INT4), data.length);
            this.buffer = LaurelinBackingArray.allocate(data.length * this.memory_itemsize());
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
        protected Array make(LaurelinBackingArray out) {
            return new Int4(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int4(this.interpretation.subarray(), this.buffer);
        }

        public int toInt(int index) {
            return this.buffer.asIntBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((int[])this.toArray());
        }

        public Int4 add(boolean bigEndian, int value) {
            LaurelinBackingArray out = LaurelinBackingArray.allocate(length * 4);
            //System.out.println(this.buffer.asIntBuffer().array());
            out.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            IntBuffer outint = out.asIntBuffer();
            IntBuffer thisint = this.buffer.asIntBuffer();
            int[] tmpout = new int[length];
            int[] tmpin = new int[length];
            thisint.get(tmpin);
            for (int i = 0;  i < this.length;  i++) {
                tmpout[i] = tmpin[i]+ value;
            }
            outint.put(tmpout);
            return new Int4(out);
        }

        public int get(int i) {
            return this.buffer.getInt(i * 4);
        }

        public void put(int i, int value) {
            this.buffer.putInt(i * 4, value);
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

        protected Int8(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
        }

        @Override
        public Object toArray(boolean bigEndian) {
            this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            LongBuffer buf = this.buffer.asLongBuffer();
            long[] out = new long[buf.limit() - buf.position()];
            buf.get(out);
            return out;
        }

        @Override
        protected Array make(LaurelinBackingArray out) {
            return new Int8(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Int8(this.interpretation.subarray(), this.buffer);
        }

        public long toLong(int index) {
            return this.buffer.asLongBuffer().get(index);
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

        protected Float4(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
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
        protected Array make(LaurelinBackingArray out) {
            return new Float4(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Float4(this.interpretation.subarray(), this.buffer);
        }

        public float toFloat(int index) {
            return this.buffer.asFloatBuffer().get(index);
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

        protected Float8(Interpretation interpretation, LaurelinBackingArray buffer) {
            super(interpretation, buffer);
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
        protected Array make(LaurelinBackingArray out) {
            return new Float8(this.interpretation, out);
        }

        @Override
        public Array subarray() {
            return new Float8(this.interpretation.subarray(), this.buffer);
        }

        public double toDouble(int index) {
            return this.buffer.asDoubleBuffer().get(index);
        }

        @Override
        public String toString() {
            return Arrays.toString((double[])this.toArray());
        }
    }
}
