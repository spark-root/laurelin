package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.interpretation.Interpretation;
import edu.vanderbilt.accre.interpretation.AsDtype;
import edu.vanderbilt.accre.array.PrimitiveArray;
import edu.vanderbilt.accre.array.RawArray;

public class PrimitiveArrayInt4 extends PrimitiveArray<PrimitiveArrayInt4> {
    public PrimitiveArrayInt4(Interpretation interpretation, int length) {
        super(interpretation, length);
    }

    public PrimitiveArrayInt4(Interpretation interpretation, RawArray rawarray) {
        super(interpretation, rawarray);
    }

    protected PrimitiveArrayInt4(Interpretation interpretation, ByteBuffer buffer) {
        super(interpretation, buffer);
    }

    public PrimitiveArrayInt4(int[] data, boolean bigEndian) {
        super(new AsDtype(AsDtype.Dtype.INT4), data.length);
        this.buffer = ByteBuffer.allocate(data.length * this.itemsize());
        this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        this.buffer.asIntBuffer().put(data, 0, data.length);
    }

    public PrimitiveArrayInt4 clip(int start, int stop) {
        return new PrimitiveArrayInt4(this.interpretation, this.rawclipped(start, stop));
    }

    public Object toArray(boolean bigEndian) {
        this.buffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        IntBuffer buf = this.buffer.asIntBuffer();
        int[] out = new int[buf.limit() - buf.position()];
        buf.get(out);
        return out;
    }
}
