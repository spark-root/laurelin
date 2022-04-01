package edu.vanderbilt.accre.laurelin.root_proxy.io;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

public class PossiblyCompressedBuf implements BackingBuf {
    private Cursor parent;
    private long base;
    private int compressedLen;
    private int uncompressedLen;
    private Reference<ByteBuffer> decompressed;

    public PossiblyCompressedBuf(Cursor parent, long base, int compressedLen, int uncompressedLen) {
        this.parent = parent;
        this.base = base; // + parent.getBase();
        this.compressedLen = compressedLen;
        this.uncompressedLen = uncompressedLen;
        this.decompressed = new SoftReference<ByteBuffer>(null);
    }

    private PossiblyCompressedBuf(Cursor parent, long base, int compressedLen, int uncompressedLen, Reference<ByteBuffer> decompressed) {
        this(parent, base, compressedLen, uncompressedLen);
        this.decompressed = decompressed;
    }

    @Override
    public ByteBuffer read(long off, long len) throws IOException {
        if (compressedLen == uncompressedLen) {
            // not compressed
            return parent.readBuffer(base + off, len);
        } else {
            ByteBuffer tmp = decompressed.get();
            if (tmp == null) {
                ByteBuffer parentBytes = parent.readBuffer(base, compressedLen);
                tmp = Compression.decompressBytes(parentBytes, compressedLen, uncompressedLen);
                decompressed = new SoftReference<ByteBuffer>(tmp);
            }

            // Make a copy first to prevent mutating the decompresed buffer
            ByteBuffer ret = tmp.duplicate();
            ret.position((int)(off));
            ret.limit((int)(off + len));
            return ret.slice();
        }
    }

    @Override
    public boolean hasLimit() throws IOException {
        return true;
    }

    @Override
    public long getLimit() throws IOException {
        return uncompressedLen;
    }

    @Override
    public BackingBuf duplicate() {
        return new PossiblyCompressedBuf(parent.duplicate(), base, compressedLen, uncompressedLen, decompressed);
    }

    /*
     * Offset to the beginning of this compressed buffer
     *
     * @returns Offset of first byte of this compressed span
     */
    public long getBufferCompressedOffset() {
        return parent.getBase();
    }

    /*
     * Returns the length of the compressed buffer before decompression
     *
     * @returns compressed buffer length
     */
    public long getBufferCompressedLen() {
        return compressedLen;
    }

    /*
     * Returns the length of the compressed buffer after decompression
     *
     * @returns decompressed buffer size
     */
    public long getBufferUncompressedLen() {
        return uncompressedLen;
    }

    /*
     * Returns the "base" of the compressed buffer, which is the offset
     * of the first byte past the compression headers (e.g. zlib or similar)
     *
     * @returns Offset of first payload byte past compression headers
     */
    public long getBufferCompressedHeaderLength() {
        return base;
    }

}
