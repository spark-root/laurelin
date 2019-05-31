package edu.vanderbilt.accre.laurelin.interpretation;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.JaggedArray;

public class AsJagged implements Interpretation {
    Interpretation content;
    int skipbytes;

    public AsJagged(Interpretation content) {
        this.content = content;
        this.skipbytes = 0;
    }

    public AsJagged(Interpretation content, int skipbytes) {
        this.content = content;
        this.skipbytes = skipbytes;
    }

    public Interpretation content() {
        return this.content;
    }

    @Override
    public int itemsize() {
        return this.content.itemsize();
    }

    @Override
    public Array empty() {
        return new JaggedArray(this, 0, new PrimitiveArray.Int4(new AsDtype(AsDtype.Dtype.INT4), 0), this.content.empty());
    }

    @Override
    public int numitems(int numbytes, int numentries) {
        return this.content.numitems(numbytes - numentries * this.skipbytes, numentries);
    }

    @Override
    public int source_numitems(Array source) {
        return this.content.source_numitems(((JaggedArray)source).content());
    }

    @Override
    public Array fromroot(RawArray bytedata, PrimitiveArray.Int4 byteoffsets, int local_entrystart, int local_entrystop) {
        RawArray compact = bytedata.compact(byteoffsets, this.skipbytes, local_entrystart, local_entrystop);

        int innersize = ((AsDtype)this.content).itemsize() * ((AsDtype)this.content).multiplicity();
        ByteBuffer countsbuf = ByteBuffer.allocate((local_entrystop - local_entrystart) * innersize);
        int total = 0;
        for (int i = local_entrystart;  i < local_entrystop;  i++) {
            int count = (byteoffsets.get(i + 1) - byteoffsets.get(i)) / innersize;
            countsbuf.putInt(count);
            total += count;
        }
        countsbuf.position(0);
        PrimitiveArray.Int4 counts = new PrimitiveArray.Int4(new RawArray(countsbuf));

        Array content = this.content.fromroot(compact, null, 0, total);
        return new JaggedArray(this, local_entrystop - local_entrystart, counts, content);
    }

    @Override
    public Array destination(int numitems, int numentries) {
        PrimitiveArray.Int4 counts = new PrimitiveArray.Int4(new AsDtype(AsDtype.Dtype.INT4), numentries);
        Array content = this.content.destination(numitems, numentries);
        return new JaggedArray(this, numentries, counts, content);
    }

    @Override
    public void fill(Array source, Array destination, int itemstart, int itemstop, int entrystart, int entrystop) {
        throw new UnsupportedOperationException("MISSING fill");
    }

    @Override
    public Array clip(Array destination, int entrystart, int entrystop) {
        throw new UnsupportedOperationException("MISSING clip");
    }

    @Override
    public Array finalize(Array destination) {
        throw new UnsupportedOperationException("MISSING finalize");
    }

    @Override
    public Interpretation subarray() {
        throw new UnsupportedOperationException("MISSING subarray");
    }

}
