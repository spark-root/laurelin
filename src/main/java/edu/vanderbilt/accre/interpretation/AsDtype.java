package edu.vanderbilt.accre.interpretation;

import java.util.Collections;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.List;
import java.lang.UnsupportedOperationException;

import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.RawArray;
import edu.vanderbilt.accre.array.PrimitiveArrayInt4;
import edu.vanderbilt.accre.array.PrimitiveArrayFloat8;
import edu.vanderbilt.accre.interpretation.Interpretation;

public class AsDtype implements Interpretation {
    enum Dtype {
        BOOL,
        INT1,
        INT2,
        INT4,
        INT8,
        UINT1,
        UINT2,
        UINT4,
        UINT8,
        FLOAT4,
        FLOAT8
    }

    Dtype dtype;
    List<Integer> dims;

    public Dtype dtype() {
        return this.dtype;
    }

    public List<Integer> dims() {
        return this.dims;
    }

    public AsDtype(Dtype dtype, List<Integer> dims) {
        this.dtype = dtype;
        this.dims = Collections.unmodifiableList(dims);
    }

    public Array empty() {
        switch (this.dtype) {
        case BOOL:
            throw new UnsupportedOperationException("not implemented yet");
        case INT1:
            throw new UnsupportedOperationException("not implemented yet");
        case INT2:
            throw new UnsupportedOperationException("not implemented yet");
        case INT4:
            throw new UnsupportedOperationException("not implemented yet");
        case INT8:
            throw new UnsupportedOperationException("not implemented yet");
        case UINT1:
            throw new UnsupportedOperationException("not implemented yet");
        case UINT2:
            throw new UnsupportedOperationException("not implemented yet");
        case UINT4:
            throw new UnsupportedOperationException("not implemented yet");
        case UINT8:
            throw new UnsupportedOperationException("not implemented yet");
        case FLOAT4:
            throw new UnsupportedOperationException("not implemented yet");
        case FLOAT8:
            return new PrimitiveArrayFloat8(this, 0);
        default:
            throw new AssertionError("unrecognized dtype");
        }
    }
    
    // public long numitems(long numbytes, long numentries) {

    // }
    
    // public long source_numitems(Array source) {

    // }
    
    // public Array fromroot(RawArray bytedata, PrimitiveArrayInt4 byteoffsets, long local_entrystart, long local_entrystop) {

    // }

    // public Array destination(long numitems, long numentries) {

    // }

    // public void fill(Array source, Array destination, long itemstart, long itemstop, long entrystart, long entrystop) {

    // }
    
    // public Array clip(Array destination, long itemstart, long itemstop, long entrystart, long entrystop) {

    // }
    
    // public Array finalize(Array destination) {

    // }
}
