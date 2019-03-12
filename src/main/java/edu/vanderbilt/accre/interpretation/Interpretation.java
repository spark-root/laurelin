package edu.vanderbilt.accre.interpretation;

import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.RawArray;
import edu.vanderbilt.accre.array.PrimitiveArray;

public interface Interpretation {
    public Array empty();
    public int numitems(int numbytes, int numentries);
    public int source_numitems(Array source);
    public Array fromroot(RawArray bytedata, PrimitiveArray.Int4 byteoffsets, int local_entrystart, int local_entrystop);
    public Array destination(int numitems, int numentries);
    public void fill(Array source, Array destination, int itemstart, int itemstop, int entrystart, int entrystop);
    public Array clip(Array destination, int itemstart, int itemstop, int entrystart, int entrystop);
    public Array finalize(Array destination);
}
