package edu.vanderbilt.accre.interpretation;

import java.lang.Integer;
import java.lang.String;
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.PrimitiveArray;
import edu.vanderbilt.accre.array.PrimitiveArrayFloat8;
import edu.vanderbilt.accre.array.PrimitiveArrayInt4;
import edu.vanderbilt.accre.array.RawArray;
import edu.vanderbilt.accre.interpretation.Interpretation;

public class AsDtype implements Interpretation {
    public enum Dtype {
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

    public AsDtype(Dtype dtype) {
        this.dtype = dtype;
        this.dims = Arrays.asList(1);
    }

    public AsDtype(Dtype dtype, List<Integer> dims) {
        this.dtype = dtype;
        this.dims = Collections.unmodifiableList(dims);
    }

    public int multiplicity() {
        int out = 1;
        for (Integer i : this.dims) {
            out *= i;
        }
        return out;
    }

    public int itemsize() {
        switch (this.dtype) {
        case BOOL:
            return 1;
        case INT1:
            return 1;
        case INT2:
            return 2;
        case INT4:
            return 4;
        case INT8:
            return 8;
        case UINT1:
            return 1;
        case UINT2:
            return 2;
        case UINT4:
            return 4;
        case UINT8:
            return 8;
        case FLOAT4:
            return 4;
        case FLOAT8:
            return 8;
        default:
            throw new AssertionError("unrecognized dtype");
        }
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
    
    public int numitems(int numbytes, int numentries) {
        if (numbytes % this.itemsize() != 0) {
            throw new AssertionError(String.format("%d byte buffer does not divide evenly into %s", numbytes, this.dtype.toString()));
        }
        return numbytes / this.itemsize();
    }
    
    public int source_numitems(Array source) {
        return ((PrimitiveArray)source).numitems();
    }
    
    public Array fromroot(RawArray bytedata, PrimitiveArrayInt4 byteoffsets, int local_entrystart, int local_entrystop) {
        if (byteoffsets != null) {
            throw new AssertionError("byteoffsets must be null for AsDtype");
        }
        int entrysize = this.multiplicity() * this.itemsize();
        RawArray sliced = bytedata.slice(local_entrystart * entrysize, local_entrystop * entrysize);
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
            return new PrimitiveArrayFloat8(this, sliced);
        default:
            throw new AssertionError("unrecognized dtype");
        }
    }

    public Array destination(int numitems, int numentries) {
        if (numitems % this.multiplicity() != 0) {
            throw new AssertionError(String.format("%d items do not divide evenly into multiplicity %d", numitems, this.multiplicity()));
        }
        int length = numitems / this.multiplicity();
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
            return new PrimitiveArrayFloat8(this, length);
        default:
            throw new AssertionError("unrecognized dtype");
        }
    }

    public void fill(Array source, Array destination, int itemstart, int itemstop, int entrystart, int entrystop) {
        ((PrimitiveArray)destination).copyitems((PrimitiveArray)source, itemstart, itemstop);
    }

    public Array clip(Array destination, int itemstart, int itemstop, int entrystart, int entrystop) {
        int mult = this.multiplicity();
        if ((itemstart % mult != 0)  ||  (itemstop % mult != 0)) {
            throw new AssertionError(String.format("itemstart (%d) or itemstop (%d) do not divide evenly into multiplicty %d", itemstart, itemstop, mult));
        }
        return destination.clip(itemstart / mult, itemstop / mult);
    }
    
    public Array finalize(Array destination) {
        return destination;
    }
}
