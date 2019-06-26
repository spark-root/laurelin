package edu.vanderbilt.accre.laurelin.interpretation;

import java.nio.ByteBuffer;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.JaggedArray;
import edu.vanderbilt.accre.laurelin.array.JaggedArrayPrep;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;

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
    public int disk_itemsize() {
        return this.content.disk_itemsize();
    }

    @Override
    public int memory_itemsize() {
        return this.content.memory_itemsize();
    }

    @Override
    public Array empty() {
        return new JaggedArrayPrep(this, 0, new PrimitiveArray.Int4(new AsDtype(AsDtype.Dtype.INT4), 0), this.content.empty());
    }

    @Override
    public int numitems(int numbytes, int numentries) {
        return this.content.numitems(numbytes - numentries * this.skipbytes, numentries);
    }

    @Override
    public int source_numitems(Array source) {
        return this.content.source_numitems(((JaggedArrayPrep)source).content());
    }

    @Override
    public Array fromroot(RawArray bytedata, PrimitiveArray.Int4 byteoffsets, int local_entrystart, int local_entrystop) {
        RawArray compact = bytedata.compact(byteoffsets, this.skipbytes, local_entrystart, local_entrystop);

        int innersize_memory = ((AsDtype)this.content).memory_itemsize() * ((AsDtype)this.content).multiplicity();
        int innersize_disk = ((AsDtype)this.content).disk_itemsize() * ((AsDtype)this.content).multiplicity();
        ByteBuffer countsbuf = ByteBuffer.allocate((local_entrystop - local_entrystart) * 4);
        int total = 0;
        for (int i = local_entrystart;  i < local_entrystop;  i++) {
            int count = (byteoffsets.get(i + 1) - byteoffsets.get(i)) / innersize_disk;
            countsbuf.putInt(count);
            total += count;
        }
        countsbuf.position(0);
        PrimitiveArray.Int4 counts = new PrimitiveArray.Int4(new RawArray(countsbuf));

        Array content = this.content.fromroot(compact, null, 0, total);
        return new JaggedArrayPrep(this, local_entrystop - local_entrystart, counts, content);
    }

    @Override
    public Array destination(int numitems, int numentries) {
        PrimitiveArray.Int4 counts = new PrimitiveArray.Int4(new AsDtype(AsDtype.Dtype.INT4), numentries);
        Array content = this.content.destination(numitems, numentries);
        return new JaggedArrayPrep(this, numentries, counts, content);
    }

    @Override
    public void fill(Array source, Array destination, int itemstart, int itemstop, int entrystart, int entrystop) {
        this.content.fill(((JaggedArrayPrep)source).content(), ((JaggedArrayPrep)destination).content(), itemstart, itemstop, entrystart, entrystop);
        ((JaggedArrayPrep)destination).counts().copyitems(((JaggedArrayPrep)source).counts(), entrystart, entrystop);
    }

    @Override
    public Array clip(Array destination, int entrystart, int entrystop) {
        PrimitiveArray.Int4 counts = (PrimitiveArray.Int4)(((JaggedArrayPrep)destination).counts().clip(entrystart, entrystop));
        // FIXME: does the content need to be explicitly clipped? Does that involve a cumsum?
        Array content = ((JaggedArrayPrep)destination).content();
        return new JaggedArrayPrep(this, counts.length(), counts, content);
    }

    @Override
    public Array finalize(Array destination) {
        PrimitiveArray.Int4 counts = ((JaggedArrayPrep)destination).counts();

        ByteBuffer offsetsbuf = ByteBuffer.allocate((counts.length() + 1) * 4);
        int last = 0;
        offsetsbuf.putInt(last);
        for (int i = 0;  i < counts.length();  i++) {
            last += counts.get(i);
            offsetsbuf.putInt(last);
        }
        offsetsbuf.position(0);
        PrimitiveArray.Int4 offsets = new PrimitiveArray.Int4(new RawArray(offsetsbuf));

        return new JaggedArray(this, counts.length(), offsets, ((JaggedArrayPrep)destination).content());
    }

    @Override
    public Interpretation subarray() {
        return this.content();
    }

    @Override
    public RawArray convertBufferDiskToMemory(RawArray source) {
        // TODO Auto-generated method stub
        return null;
    }

}
