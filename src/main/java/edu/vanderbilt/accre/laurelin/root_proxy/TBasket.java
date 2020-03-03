package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.root_proxy.io.Cursor;

public class TBasket {
    private static final Logger logger = LogManager.getLogger();

    private TKey key;
    // https://root.cern.ch/doc/master/TBasket_8h_source.html
    private short vers;
    private int fBufferSize;
    private int fNevBufSize;
    private int fNevBuf;
    private int fLast;
    private byte fHeaderOnly;
    private int fBasketBytes;
    private long fBasketEntry;
    private long fBasketSeek;
    private Cursor startCursor;
    private Cursor payload;

    public TBasket(Cursor cursor, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
        this.fBasketBytes = fBasketBytes;
        this.fBasketEntry = fBasketEntry;
        this.fBasketSeek = fBasketSeek;
        startCursor = cursor.duplicate();
        key = new TKey();
        Cursor c = key.getFromFile(cursor);
        vers = c.readShort();
        fBufferSize = c.readInt();
        fNevBufSize = c.readInt();
        fNevBuf = c.readInt();
        fLast = c.readInt();
        fHeaderOnly = c.readChar();
    }

    public static TBasket getFromFile(Cursor cursor, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
        TBasket ret = new TBasket(cursor, fBasketBytes, fBasketEntry, fBasketSeek);
        return ret;
    }

    private void initializePayload() {
        // FIXME mutex this eventually
        this.payload = startCursor.getPossiblyCompressedSubcursor(key.KeyLen,
                key.Nbytes - key.KeyLen,
                key.ObjLen,
                key.KeyLen);
        logger.trace("get basket: " + key.fName);
    }

    public ByteBuffer getPayload(long offset, int len) throws IOException {
        if (this.payload == null) {
            initializePayload();
        }
        return this.payload.readBuffer(offset, len);
    }

    public ByteBuffer getPayload() throws IOException {
        if (this.payload == null) {
            initializePayload();
        }
        long len = payload.getLimit();
        return this.payload.readBuffer(0, len);
    }

    public int getBasketBytes() {
        return fBasketBytes;
    }

    public int getKeyLen() {
        return key.KeyLen;
    }

    /**
     *
     * @return last event number
     */
    public int getLast() {
        return this.fLast;
    }

    public int getObjLen() {
        return key.ObjLen;
    }

    public int getNevBuf() {
        return fNevBuf;
    }

    public int getNevBufSize() {
        return fNevBufSize;
    }

    public long getBasketEntry() {
        return fBasketEntry;
    }
}
