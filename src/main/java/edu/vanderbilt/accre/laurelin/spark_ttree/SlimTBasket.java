package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.root_proxy.TBranch.CompressedBasketInfo;
import edu.vanderbilt.accre.laurelin.root_proxy.TKey;
import edu.vanderbilt.accre.laurelin.root_proxy.io.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.io.InMemoryBackingBuf;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFile;

public class SlimTBasket implements Serializable {
    private static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = 1L;
    private long offset;
    private Cursor payload;
    private TKey key;

    private boolean isPopulated = false;
    private int compressedLen;
    private int uncompressedLen;
    private int keyLen;
    private int last;

    private short vers;
    private int fBufferSize;
    private int fNevBufSize;
    private int fNevBuf;
    private byte fHeaderOnly;
    private Cursor headerEnd;

    private CompressedBasketInfo compressedBasketInfo;

    SlimTBasket(long offset) {
        this.offset = offset;
    }

    SlimTBasket(long offset, CompressedBasketInfo compressedBasketInfo) {
        this(offset);
        this.setCompressedBasketInfo(compressedBasketInfo);
    }

    public static SlimTBasket makeEagerBasket(SlimTBranchInterface branch, long offset, int compressedLen, int uncompressedLen, int keyLen, int last) {
        SlimTBasket ret = new SlimTBasket(offset);
        ret.isPopulated = true;
        ret.compressedLen = compressedLen;
        ret.uncompressedLen = uncompressedLen;
        ret.keyLen = keyLen;
        ret.last = last;
        return ret;
    }

    /*
     * Make a lazy basket with the accompanying offset and compressed location
     */
    public static SlimTBasket makeLazyBasket(long offset, CompressedBasketInfo compressedBasketInfo) {
        SlimTBasket ret = new SlimTBasket(offset, compressedBasketInfo);
        ret.isPopulated = false;
        return ret;
    }

    /*
     *  Make a lazy basket with an offset, but is incompatible with baskets stored
     *  within a compressed range, which can happen with embedded baskets
     */
    public static SlimTBasket makeLazyBasket(long offset) {
        return makeLazyBasket(offset, null);
    }

    public synchronized void initializeMetadata(ROOTFile tmpFile) {
        if (isPopulated == false) {
            try {
                key = new TKey();
                Cursor cursor;
                if (getCompressedBasketInfo() != null) {
                    CompressedBasketInfo loc = getCompressedBasketInfo();
                    cursor = tmpFile.getCursor(loc.getCompressedParentOffset());
                    cursor = cursor.getPossiblyCompressedSubcursor(loc.getHeaderLen(),
                                                         loc.getCompressedLen(),
                                                         loc.getUncompressedLen(),
                                                         loc.getHeaderLen());
                    cursor.setOffset(loc.getStartOffset());
                    Cursor c = key.getFromFile(cursor);
                    uncompressedLen = loc.getBasketLen();
                    compressedLen = loc.getBasketLen();
                    keyLen = key.getKeyLen();
                    vers = c.readShort();
                    fBufferSize = c.readInt();
                    fNevBufSize = c.readInt();
                    fNevBuf = c.readInt();
                    last = c.readInt();
                    fHeaderOnly = c.readChar();
                    headerEnd = c;
                } else {
                    cursor = tmpFile.getCursor(offset);
                    Cursor c = key.getFromFile(cursor);
                    keyLen = key.getKeyLen();
                    compressedLen = key.getNBytes() - key.getKeyLen();
                    uncompressedLen = key.getObjLen();
                    vers = c.readShort();
                    fBufferSize = c.readInt();
                    fNevBufSize = c.readInt();
                    fNevBuf = c.readInt();
                    last = c.readInt();
                    fHeaderOnly = c.readChar();
                    headerEnd = c;
                }
                isPopulated = true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getKeyLen() {
        if (isPopulated == false) {
            throw new RuntimeException("Slim basket not initialized");
        }
        return keyLen;
    }

    public int getObjLen() {
        if (isPopulated == false) {
            throw new RuntimeException("Slim basket not initialized");
        }
        return uncompressedLen;
    }

    public int getLast() {
        if (isPopulated == false) {
            throw new RuntimeException("Slim basket not initialized");
        }
        return last;
    }

    public long getOffset() {
        return offset;
    }

    public ByteBuffer getPayload(ROOTFile tmpFile) throws IOException {
        initializeMetadata(tmpFile);
        if (this.payload == null) {
            initializePayload(tmpFile);
        }
        return this.payload.readBuffer(0, uncompressedLen);
    }

    private void initializePayload(ROOTFile tmpFile) throws IOException {
        if (isPopulated == false) {
            throw new RuntimeException("Slim basket not initialized");
        }
        Cursor cursor;
        TKey key2 = new TKey();
        if (getCompressedBasketInfo() != null) {
//            CompressedBasketInfo loc = compressedBasketInfo;
//            cursor = tmpFile.getCursor(loc.getCompressedParentOffset());
//            cursor = cursor.getPossiblyCompressedSubcursor(loc.getHeaderLen(),
//                                                 loc.getCompressedLen(),
//                                                 loc.getUncompressedLen(),
//                                                 loc.getHeaderLen());
//            cursor.setOffset(offset + keyLen);
        	CompressedBasketInfo info = getCompressedBasketInfo();
        	Cursor top = headerEnd.duplicate();
        	int size = info.getLast() - info.getKeyLen();

        	Cursor offsets = null;
        	int offsetLen = info.getNevBuf() * 4 + 8;
        	if (info.getNevBufSize() > 8) {
        		// Make a copy of the contents
        		offsets = top.duplicate();
        		top.skipBytes(offsetLen - 4);
        	}
        	top.skipBytes(info.getKeyLen());

        	Cursor contents = top.duplicate();
        	if (offsets != null) {
        	    /*
        	     * Embedded/recovered baskets aren't layed out on disk the same as
        	     * regular/"loose"  baskets, so we need to move bytes around to get
        	     * things looking the same
        	     */
        		ByteBuffer offsetBytes = offsets.readBuffer(offsetLen);
        		ByteBuffer contentBytes = contents.readBuffer(size);
        		size = offsetLen + size;
        		ByteBuffer concat = ByteBuffer.allocate(offsetLen + size);
        		concat.put(offsetBytes);
        		concat.put(contentBytes);
        		InMemoryBackingBuf concatBuf = new InMemoryBackingBuf(concat);
        		concat.rewind();
        		contents = new Cursor(concatBuf, 0);
        		contents.setOffset(0);
        	}
            this.payload = contents;
            this.uncompressedLen = size;

//            Cursor c = headerEnd.duplicate();
//            key2.getFromFile(c);
        } else {
            this.payload = tmpFile.getCursor(offset).getPossiblyCompressedSubcursor(keyLen,
                    compressedLen,
                    uncompressedLen,
                    keyLen);
            // COPY THE OFFSETS AND CONTENT OVER
        }
    }

    public CompressedBasketInfo getCompressedBasketInfo() {
        return compressedBasketInfo;
    }

    public void setCompressedBasketInfo(CompressedBasketInfo compressedBasketInfo) {
        this.compressedBasketInfo = compressedBasketInfo;
    }
}