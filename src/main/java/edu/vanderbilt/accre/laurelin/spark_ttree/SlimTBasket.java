package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.root_proxy.TKey;
import edu.vanderbilt.accre.laurelin.root_proxy.io.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFile;

public class SlimTBasket implements Serializable {
    private static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = 1L;
    private long offset;
    private Cursor payload;


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

    SlimTBasket(long offset) {
        this.offset = offset;
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

    public static SlimTBasket makeLazyBasket(long offset) {
        SlimTBasket ret = new SlimTBasket(offset);
        ret.isPopulated = false;
        return ret;
    }

    public synchronized void initializeMetadata(ROOTFile tmpFile) {
        if (isPopulated == false) {
            try {
                Cursor cursor = tmpFile.getCursor(offset);
                TKey key = new TKey();
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
        Cursor fileCursor = tmpFile.getCursor(offset);
        if (isPopulated == false) {
            throw new RuntimeException("Slim basket not initialized");
        }
        this.payload = fileCursor.getPossiblyCompressedSubcursor(keyLen,
                compressedLen,
                uncompressedLen,
                keyLen);
    }
}