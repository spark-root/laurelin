package edu.vanderbilt.accre.laurelin.array;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class JaggedArray extends Array {
    PrimitiveArray.Int4 counts;
    PrimitiveArray.Int4 offsets;
    Array content;

    public JaggedArray(Interpretation interpretation, int length, PrimitiveArray.Int4 counts, Array content) {
        super(interpretation, length);
        this.counts = counts;
        this.content = content;
        this.offsets = null;
    }

    public PrimitiveArray.Int4 counts() {
        return this.counts;
    }

    public Array content() {
        return this.content;
    }

    public PrimitiveArray.Int4 offsets() {
        if (this.offsets == null) {
            allocateOffsets();
        }
        return offsets;
    }
    private void allocateOffsets() {
        LaurelinBackingArray offsetsbuf = LaurelinBackingArray.allocate((this.counts.length() + 1) * 4);
        int last = 0;
        int startpos = this.counts.get(0);
        offsetsbuf.putInt(last);
        for (int i = 0; i < this.counts.length(); i++) {
            last += this.counts.get(i);
            offsetsbuf.putInt(last);
        }
        offsetsbuf.flip();
        this.offsets = new PrimitiveArray.Int4(new RawArray(offsetsbuf));
    }

    @Override
    public Array clip(int start, int stop) {
        if (this.offsets == null) {
            allocateOffsets();
        }
        int itemstart = this.offsets.get(start);
        int itemstop = this.offsets.get(stop);
        return new JaggedArray(this.interpretation, stop - start, (PrimitiveArray.Int4)this.counts.clip(start, stop), this.content.clip(itemstart, itemstop));
    }

    @Override
    public Object toArray(boolean bigEndian) {

        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Array subarray() {
        if (this.offsets == null) {
            allocateOffsets();
        }
        int low = this.offsets.get(0);
        int high = this.offsets.get(1);
        Array ret = this.content.clip(low, high);
        return ret;
    }

}
