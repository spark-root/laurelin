package edu.vanderbilt.accre.laurelin.interpretation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;

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

    @Override
    public int disk_itemsize() {
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

    @Override
    public int memory_itemsize() {
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
                return 2;
            case UINT2:
                return 4;
            case UINT4:
                return 8;
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

    @Override
    public Array empty() {
        switch (this.dtype) {
            case BOOL:
                throw new UnsupportedOperationException("not implemented yet");
            case INT1:
                return new PrimitiveArray.Int1(this, 0);
            case INT2:
                return new PrimitiveArray.Int2(this, 0);
            case INT4:
                return new PrimitiveArray.Int4(this, 0);
            case INT8:
                return new PrimitiveArray.Int8(this, 0);
            case UINT1:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT2:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT4:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT8:
                throw new UnsupportedOperationException("not implemented yet");
            case FLOAT4:
                return new PrimitiveArray.Float4(this, 0);
            case FLOAT8:
                return new PrimitiveArray.Float8(this, 0);
            default:
                throw new AssertionError("unrecognized dtype");
        }
    }

    @Override
    public int numitems(int numbytes, int numentries) {
        if (numbytes % this.disk_itemsize() != 0) {
            throw new AssertionError(
                    String.format("%d byte buffer does not divide evenly into %s", numbytes, this.dtype.toString()));
        }
        return numbytes / this.disk_itemsize();
    }

    @Override
    public int source_numitems(Array source) {
        return ((PrimitiveArray) source).numitems();
    }

    @Override
    public Array fromroot(RawArray bytedata, PrimitiveArray.Int4 byteoffsets, int local_entrystart, int local_entrystop) {
        if (byteoffsets != null) {
            throw new AssertionError("byteoffsets must be null for AsDtype");
        }
        int entrysize = this.multiplicity() * this.disk_itemsize();
        RawArray sliced = bytedata.slice(local_entrystart * entrysize, local_entrystop * entrysize);
        switch (this.dtype) {
            case BOOL:
                return new PrimitiveArray.Bool(this, sliced);
            case INT1:
                return new PrimitiveArray.Int1(this, sliced);
            case INT2:
                return new PrimitiveArray.Int2(this, sliced);
            case INT4:
                return new PrimitiveArray.Int4(this, sliced);
            case INT8:
                return new PrimitiveArray.Int8(this, sliced);
            case UINT1:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT2:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT4:
                throw new UnsupportedOperationException("not implemented yet");
            case UINT8:
                throw new UnsupportedOperationException("not implemented yet");
            case FLOAT4:
                return new PrimitiveArray.Float4(this, sliced);
            case FLOAT8:
                return new PrimitiveArray.Float8(this, sliced);
            default:
                throw new AssertionError("unrecognized dtype");
        }
    }

    @Override
    public Array destination(int numitems, int numentries) {
        if (numitems % this.multiplicity() != 0) {
            throw new AssertionError(
                    String.format("%d items do not divide evenly into multiplicity %d", numitems, this.multiplicity()));
        }
        int length = numitems / this.multiplicity();
        switch (this.dtype) {
            case BOOL:
                return new PrimitiveArray.Bool(this, length);
            case INT1:
                return new PrimitiveArray.Int1(this, length);
            case INT2:
                return new PrimitiveArray.Int2(this, length);
            case INT4:
                return new PrimitiveArray.Int4(this, length);
            case INT8:
                return new PrimitiveArray.Int8(this, length);
            case UINT1:
                return new PrimitiveArray.Int2(this, length);
            case UINT2:
                return new PrimitiveArray.Int4(this, length);
            case UINT4:
                return new PrimitiveArray.Int8(this, length);
            case UINT8:
                return new PrimitiveArray.Float8(this, length);
            case FLOAT4:
                return new PrimitiveArray.Float4(this, length);
            case FLOAT8:
                return new PrimitiveArray.Float8(this, length);
            default:
                throw new AssertionError("unrecognized dtype");
        }
    }

    @Override
    public void fill(Array source, Array destination, int itemstart, int itemstop, int entrystart, int entrystop) {
        ((PrimitiveArray) destination).copyitems((PrimitiveArray) source, itemstart, itemstop);
    }

    @Override
    public Array clip(Array destination, int entrystart, int entrystop) {
        return destination.clip(entrystart, entrystop);
    }

    @Override
    public Array finalize(Array destination) {
        return destination;
    }

    @Override
    public Interpretation subarray() {
        if (this.dims.size() == 0) {
            throw new IllegalArgumentException("trying to take the subarray of a scalar type");
        }
        return new AsDtype(this.dtype, this.dims.subList(1, this.dims.size()));
    }
}
