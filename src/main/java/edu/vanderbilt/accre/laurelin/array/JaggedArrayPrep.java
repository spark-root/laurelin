package edu.vanderbilt.accre.laurelin.array;

import java.lang.UnsupportedOperationException;
import java.nio.ByteBuffer;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class JaggedArrayPrep extends Array {
    PrimitiveArray.Int4 counts;
    PrimitiveArray.Int4 offsets;
    Array content;

    public JaggedArrayPrep(Interpretation interpretation, int length, PrimitiveArray.Int4 counts, Array content) {
        super(interpretation, length);
        this.counts = counts;
        this.offsets = null;
        this.content = content;
    }

    public PrimitiveArray.Int4 counts() {
        return this.counts;
    }

    public Array content() {
        return this.content;
    }

    public Array clip(int start, int stop) {
        if (this.offsets == null) {
            ByteBuffer offsetsbuf = ByteBuffer.allocate((this.counts.length() + 1) * 4);
            int last = 0;
            offsetsbuf.putInt(last);
            for (int i = 0;  i < this.counts.length();  i++) {
                last += this.counts.get(i);
                offsetsbuf.putInt(last);
            }
            offsetsbuf.position(0);
            this.offsets = new PrimitiveArray.Int4(new RawArray(offsetsbuf));
        }

        int itemstart = this.offsets.get(start);
        int itemstop = this.offsets.get(stop);
        return this.content.clip(itemstart, itemstop);
    }

    public Object toArray(boolean bigEndian) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Array subarray() {
        throw new UnsupportedOperationException("not implemented yet");
    }

}
