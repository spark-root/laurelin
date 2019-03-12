package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.ByteOrder;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.interpretation.AsDtype;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class PrimitiveArrayFloat8 extends PrimitiveArray<PrimitiveArrayFloat8> {
    public PrimitiveArrayFloat8(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public PrimitiveArrayFloat8(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray);
    }

    protected PrimitiveArrayFloat8(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, buffer);
    }

    public PrimitiveArrayFloat8(double[] data, boolean bigEndian) {
        super(new AsDtype(AsDtype.Dtype.FLOAT8), data.length);
        this.buffer = ByteBuffer.allocate(data.length * this.itemsize());
        this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        this.buffer.asDoubleBuffer().put(data, 0, data.length);
    }

    public PrimitiveArrayFloat8 clip(int start, int stop) {
        return new PrimitiveArrayFloat8(this.interpretation, this.rawclipped(start, stop));
    }

    public Object toArray(boolean bigEndian) {
        this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        DoubleBuffer buf = this.buffer.asDoubleBuffer();
        double[] out = new double[buf.limit() - buf.position()];
        buf.get(out);
        return out;
    }
}
