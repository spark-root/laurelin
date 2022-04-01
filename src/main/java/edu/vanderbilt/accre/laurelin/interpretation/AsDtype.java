package edu.vanderbilt.accre.laurelin.interpretation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;

public class AsDtype implements Interpretation {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();
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
        return memory_itemsize(this.dtype);
    }

    public static int memory_itemsize(Dtype dtype) {
        switch (dtype) {
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
                return new PrimitiveArray.Int2(this, 0);
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
        int entrysize = this.multiplicity() * this.memory_itemsize();
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
                return new PrimitiveArray.Int2(this, sliced);
            case UINT2:
                return new PrimitiveArray.Int4(this, sliced);
            case UINT4:
                return new PrimitiveArray.Int8(this, sliced);
            case UINT8:
                return new PrimitiveArray.Int8(this, sliced);
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
                return new PrimitiveArray.Int8(this, length);
            case FLOAT4:
                return new PrimitiveArray.Float4(this, length);
            case FLOAT8:
                return new PrimitiveArray.Float8(this, length);
            default:
                throw new AssertionError("unrecognized dtype");
        }
    }

    @Override
    public RawArray convertBufferDiskToMemory(RawArray source) {
        RawArray converted;
        switch (this.dtype) {
            case UINT1:
                /*
                 * Conveniently both Java and ROOT are big-endian, to convert
                 * the following unsigned 8-bit int into a signed 16-bit int,
                 * add zeros like the following
                 *
                 * index:  0  1  2  3  4  5  6  7  8  9
                 * src:   11 22 33 44
                 * dest:  00 11 00 22 00 33 00 44
                 *
                 * ByteBuffers are always initialized to zero
                 */
                converted = new RawArray(source.length() * 2);
                for (int i = 0; i < source.length(); i += 1) {
                    converted.put((i * 2) + 1, source.getByte(i));
                }
                return new RawArray(converted);
            case UINT2:
                /*
                 * index:  0  1  2  3  4  5  6  7  8  9
                 * src:   11 22 33 44
                 * dest:  00 00 11 22 00 00 33 44
                 */
                converted = new RawArray(source.length() * 2);
                for (int i = 0; i < source.length(); i += 2) {
                    converted.put((i * 2) + 2, source.getByte(i));
                    converted.put((i * 2) + 3, source.getByte(i + 1));
                }
                return new RawArray(converted);
            case UINT4:
                /*
                 * index:  0  1  2  3  4  5  6  7  8  9
                 * src:   11 22 33 44
                 * dest:  00 00 00 00 11 22 33 44
                 */
                converted = new RawArray(source.length() * 2);
                for (int i = 0; i < source.length(); i += 4) {
                    converted.put((i * 2) + 4, source.getByte(i));
                    converted.put((i * 2) + 5, source.getByte(i + 1));
                    converted.put((i * 2) + 6, source.getByte(i + 2));
                    converted.put((i * 2) + 7, source.getByte(i + 3));
                }
                return new RawArray(converted);
            case UINT8:
                /*
                 * There is no type in Spark that can represent the full range
                 * of 8-byte unsigned integers, so we need to cast it to a
                 * long
                 */
                converted = new RawArray(source.length());
                for (int i = 0; i < source.length(); i += 1) {
                    converted.put(source.getByte(i));
                }
                return new RawArray(converted);
            default:
                break;
        }
        return source;
    }

    @Override
    public PrimitiveArray.Int4 convertOffsetDiskToMemory(PrimitiveArray.Int4 source) {
        PrimitiveArray.Int4 converted;
        switch (this.dtype) {
            case UINT1:
            case UINT2:
            case UINT4:
                /*
                 * The offsets are byte-indexed so if we make the type larger,
                 * we need to also re-point the offsets
                 */
                converted = new PrimitiveArray.Int4(source.length());
                for (int i = 0; i < source.length(); i += 1) {
                    converted.put(i, source.get(i) * 2);
                }
                return converted;
            default:
                break;
        }
        return source;
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
