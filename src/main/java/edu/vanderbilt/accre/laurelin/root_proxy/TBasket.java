package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.root_proxy.TBranch.CompressedBasketInfo;
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
    private boolean embedded;

    private CompressedBasketInfo loc;

    public static TBasket getLooseFromFile(Cursor cursor, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
        TBasket ret = new TBasket();
        ret.fBasketBytes = fBasketBytes;
        ret.fBasketEntry = fBasketEntry;
        ret.fBasketSeek = fBasketSeek;
        ret.startCursor = cursor.duplicate();
        ret.key = new TKey();
        Cursor c = ret.key.getFromFile(cursor);
        ret.vers = c.readShort();
        ret.fBufferSize = c.readInt();
        ret.fNevBufSize = c.readInt();
        ret.fNevBuf = c.readInt();
        ret.fLast = c.readInt();
        ret.fHeaderOnly = c.readChar();
        ret.embedded = false;
        return ret;
    }

    public static TBasket getEmbeddedFromFile(Cursor cursor, CompressedBasketInfo loc, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
        TBasket ret = new TBasket();
        ret.fBasketBytes = fBasketBytes;
        ret.fBasketEntry = fBasketEntry;
        ret.fBasketSeek = fBasketSeek;
        ret.startCursor = cursor.duplicate();
        ret.key = new TKey();
        Cursor c = ret.key.getFromFile(cursor);
        ret.vers = c.readShort();
        ret.fBufferSize = c.readInt();
        ret.fNevBufSize = c.readInt();
        ret.fNevBuf = c.readInt();
        ret.fLast = c.readInt();
        ret.fHeaderOnly = c.readChar();
        ret.loc = loc;
        ret.embedded = true;
        return ret;
    }

    private void initializePayload() {
        // FIXME mutex this eventually
    	https://github.com/scikit-hep/uproot3/blob/54f5151fb7c686c3a161fbe44b9f299e482f346b/uproot3/tree.py#L1763-L1769
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

	public long getBasketSeek() {
		return fBasketSeek;
	}
}
