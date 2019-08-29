package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Represents a pseudo "file pointer" into pluggable backings. The ROOTFile
 * interface will return a Cursor object that lets the user deserialize
 * different "POD"-esque types like floats, ints, but also more complicated
 * types like c-strings, etc.. The cursor object keeps track of its current
 * offset, so multiple cursors can be used in a thread-safe manner. Sub-cursors
 * can be created from other cursors to handle (e.g.) accessing decompressed
 * byte ranges using the same cursor interface. For future buffer management,
 * cursors track their parents up to the ultimate truth interface. This will
 * let us eventually store soft-references to underlying ByteBuffer objects
 * so they can be more easily GCd by java
 */
public class Cursor {
    /*
     * Visually - in the case of reading from a file:
     *
     * Buf     |    1                      |
     *
     * Parent       |       2              |
     *
     * Child                |     3        |
     *
     * 1: Parent.base
     * 2: Child.base
     * 3: Child.base + Child.off
     *
     */



    /**
     * Where "zero" is for this buffer in absolute coordinates (e.g. not
     * relative to the parent). Sub-cursors start at their initialized
     * position
     */
    protected long base;

    /**
     * Needed to get Streamers working
     */
    protected long origin;

    /**
     * The current offset into this buffer
     */
    protected long off;

    private BackingBuf buf;
    private Cursor parent;

    public Cursor(BackingBuf impl, long base) {
        this.buf = impl;
        this.base = base;
        this.origin = 0;
    }

    public Cursor getParent() {
        return parent;
    }

    /**
     *
     * @return The oldest ancestor of this cursor
     */
    public Cursor getOldestParent() {
        if (parent == null) {
            return this;
        } else {
            return parent.getParent();
        }
    }

    public Cursor duplicate() {
        Cursor ret = new Cursor(buf.duplicate(), base);
        ret.setOffset(off);
        if (this.parent != null) {
            ret.parent = this.parent.duplicate();
        }
        ret.origin = this.origin;
        return ret;
    }

    public Cursor getSubcursor(long off) {
        Cursor tmp = this.duplicate();
        tmp.setOffset(off);
        return tmp;
    }

    public long getOrigin() {
        return origin;
    }

    public long getBase() {
        if (parent != null) {
            return base + parent.getBase();
        } else {
            return base;
        }
    }

    public long getLimit() throws IOException {
        return buf.getLimit();
    }

    /*
     * Stolen from uproot
     */
    // CHECKSTYLE:OFF
    //	def _startcheck(source, cursor):
    //	    start = cursor.index
    //	    cnt, vers = cursor.fields(source, _startcheck._format_cntvers)
    //	    if numpy.int64(cnt) & uproot.const.kByteCountMask:
    //	        cnt = int(numpy.int64(cnt) & ~uproot.const.kByteCountMask)
    //	        return start, cnt + 4, vers
    //	    else:
    //	        cursor.index = start
    //	        vers, = cursor.fields(source, _startcheck._format_cntvers2)
    //	        return start, None, vers
    //	_startcheck._format_cntvers = struct.Struct(">IH")
    //	_startcheck._format_cntvers2 = struct.Struct(">H")
    // CHECKSTYLE:ON
    public void startCheck() throws IOException {
        long off = getOffset();
        long cnt = readUInt();
        //CHECKSTYLE:OFF
        // need to read the short even though it's not used
        int vers = readUShort();
        //CHECKSTYLE:ON
        if ((cnt & ~ Constants.kByteCountMask) != 0) {
            cnt = cnt & ~ Constants.kByteCountMask;
        } else {
            setOffset(off);
            vers = readUShort();
        }
    }

    /**
     * Gets a subcursor that might be compressed. You can tell if it's
     * compressed if compressedLen != uncompressedLen. If that's true, read
     * ROOT's "compression frame" to get algorithm &amp; parameters
     *
     * @param off Beginning of this possibly compressed byterange
     * @param compressedLen Length of this byterange in the parent
     * @param uncompressedLen Length of this byterange after (possible) decompression
     * @param keyLen length of the key to skip
     * @return Cursor pointing into new byterange
     */
    public Cursor getPossiblyCompressedSubcursor(long off, int compressedLen, int uncompressedLen, int keyLen) {
        if (compressedLen == uncompressedLen) {
            // If the object is uncompressed, just make a new subcursor that
            // points into this current one
            Cursor ret = this.duplicate();
            ret.base = off +  this.off + this.base;
            ret.off = keyLen;
            ret.origin = -keyLen;
            return ret;
        } else {
            BackingBuf bbuf = new PossiblyCompressedBuf(this, off, compressedLen, uncompressedLen);
            Cursor ret = new Cursor(bbuf, 0);
            ret.origin = -keyLen;
            ret.parent = this;
            return ret;
        }
    }

    public long getOffset() {
        return off;
    }

    public void setOffset(long newoff) {
        off = newoff;
    }

    public void skipBytes(long amount) {
        off += amount;
    }

    public ByteBuffer readBuffer(long offset, long len) throws IOException {
        return buf.read(base + offset, len);
    }

    public ByteBuffer readBuffer(long len) throws IOException {
        ByteBuffer ret = buf.read(base + off, len);
        off += ret.limit();
        return ret;
    }
    /*
     * Map 1,2,4,8-byte (un)signed integers to java types
     */


    public byte readChar(long offset) throws IOException {
        return buf.read(base + offset, 1).get(0);
    }

    public byte readChar() throws IOException {
        byte ret = readChar(off);
        off += 1;
        return ret;
    }

    public short readUChar(long offset) throws IOException {
        short ret = buf.read(base + offset, 1).get(0);
        if (ret < 0)
            ret += 256;
        return ret;
    }

    public short readUChar() throws IOException {
        short ret = readUChar(off);
        off += 1;
        return ret;
    }

    public short readShort(long offset) throws IOException {
        return buf.read(base + offset, 2).getShort(0);
    }

    public short readShort() throws IOException {
        short ret = readShort(off);
        off += 2;
        return ret;
    }

    public int readUShort(long offset) throws IOException {
        int ret = buf.read(base + offset, 2).getShort(0);
        if (ret < 0) {
            ret += 65536L;
        }
        return ret;
    }

    public int readUShort() throws IOException {
        int ret = readUShort(off);
        off += 2;
        return ret;
    }

    public int readInt(long offset) throws IOException {
        return buf.read(base + offset, 4).getInt(0);
    }

    public int readInt() throws IOException {
        int ret = readInt(off);
        off += 4;
        return ret;
    }

    public int[] readIntArray(int len) throws IOException {
        int []ret = new int[len];
        buf.read(off + base, len * 4).asIntBuffer().get(ret, 0, len);
        off += len * 4;
        return ret;
    }

    public long readUInt(long offset) throws IOException {
        long ret = buf.read(base + offset, 4).getInt(0);
        if (ret < 0) {
            ret += 4294967296L;
        }
        return (ret & 0xFFFFFFFFL);
    }

    public long readUInt() throws IOException {
        long ret = readUInt(off);
        off += 4;
        return ret;
    }

    public long readLong(long offset) throws IOException {
        return buf.read(base + offset, 8).getLong(0);
    }

    public long readLong() throws IOException {
        long ret = readLong(off);
        off += 8;
        return ret;
    }

    public long[] readLongArray(int len) throws IOException {
        long []ret = new long[len];
        buf.read(base + off, len * 8).asLongBuffer().get(ret, 0, len);
        off += len * 8;
        return ret;
    }

    public BigInteger readULong(long offset) throws IOException {
        byte[] tmpArray = new byte[8];
        ByteBuffer tmpBuf = buf.read(base + offset, 8);
        tmpBuf.get(tmpArray, 0, 8);
        BigInteger ret = new BigInteger(tmpArray);
        if (ret.compareTo(BigInteger.ZERO) == -1) {
            ret = ret.add(BigInteger.valueOf(2).pow(64));
        }
        return ret;
    }

    public BigInteger readULong() throws IOException {
        BigInteger ret = readULong(off);
        off += 8;
        return ret;
    }

    /*
     * Floating point values
     */
    public float readFloat(long offset) throws IOException {
        return buf.read(base + offset, 4).getFloat(0);
    }

    public float readFloat() throws IOException {
        float ret = readFloat(off);
        off += 4;
        return ret;
    }

    public double readDouble(long offset) throws IOException {
        return buf.read(base + offset, 8).getDouble(0);
    }

    public double readDouble() throws IOException {
        double ret = readDouble(off);
        off += 8;
        return ret;
    }

    public double[] readDoubleArray(int len) throws IOException {
        double []ret = new double[len];
        buf.read(base + off, len * 8).asDoubleBuffer().get(ret, 0, len);
        off += len * 8;
        return ret;
    }

    public String readTString(long offset) throws IOException {
        int l = readUChar(offset);
        offset += 1;

        if (l == 255) {
            l = readInt(offset);
            offset += 4;
        }
        ByteBuffer bytes = buf.read(base + offset, l);
        byte[] rawbytes = new byte[l];
        bytes.position(0);
        bytes.get(rawbytes, 0, l);
        String ret;
        if (l == 0) {
            ret = new String();
        } else {
            ret = new String(rawbytes);
        }
        if (l != ret.length()) {
            throw new RuntimeException("Weird string length");
        }
        return ret;
    }

    public String readTString() throws IOException {
        String ret = readTString(off);
        if (ret.length() < 255) {
            // only one length byte
            off += ret.length() + 1;
        } else {
            // first length byte is 255, then the next 4 bytes for the rest
            off += ret.length() + 1 + 4;
        }
        return ret;
    }

    public String readCString(long offset) throws IOException {
        int bufSize = 1024;
        boolean done = false;
        long buf_start = offset;
        ByteBuffer tmpbuf;
        String ret = new String();
        while (!done) {
            int real_len = bufSize;
            if (buf.hasLimit()) {
                real_len = Math.min(bufSize, (int)(buf.getLimit() - base - buf_start));
                done = true;
            }
            tmpbuf = buf.read(base + buf_start, real_len);
            int x;
            for (x = 0; x < tmpbuf.limit() && tmpbuf.get(x) != '\0'; x += 1) {
                ret += (char) tmpbuf.get(x);
            }
            if (tmpbuf.get(x) == '\0') {
                break;
            }
            buf_start += bufSize;
        }
        return ret;
    }

    public String readCString() throws IOException {
        String ret = readCString(off);
        // Skip the null byte
        off += ret.length() + 1;
        return ret;
    }

}

