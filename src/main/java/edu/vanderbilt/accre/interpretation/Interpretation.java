package edu.vanderbilt.accre.interpretation;

import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.RawArray;
import edu.vanderbilt.accre.array.PrimitiveArrayInt4;

public interface Interpretation {
    public Array empty();
    public long numitems(long numbytes, long numentries);
    // public long source_numitems(Array source);
    // public Array fromroot(RawArray bytedata, PrimitiveArrayInt4 byteoffsets, long local_entrystart, long local_entrystop);
    // public Array destination(long numitems, long numentries);
    // public void fill(Array source, Array destination, long itemstart, long itemstop, long entrystart, long entrystop);
    // public Array clip(Array destination, long itemstart, long itemstop, long entrystart, long entrystop);
    // public Array finalize(Array destination);
}
