package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TBasket {
	private TKey key;
	// https://root.cern.ch/doc/master/TBasket_8h_source.html
	private short vers;
	private int fBufferSize;
	private int fNevBufSize;
	private int fNevBuf;
	private int fLast;
	private byte fHeaderOnly;
	private Cursor headerEnd;
	private int fBasketBytes;
	private long fBasketEntry;
	private long fBasketSeek;
	private Cursor payload;
	
	public TBasket(Cursor cursor, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
		this.fBasketBytes = fBasketBytes;
		this.fBasketEntry = fBasketEntry;
		this.fBasketSeek = fBasketSeek;
		key = new TKey();
		key.getFromFile(cursor);
		Cursor c = key.getEndCursor().duplicate();
		vers = c.readShort();
		fBufferSize = c.readInt();
		fNevBufSize = c.readInt();
		fNevBuf = c.readInt();
		fLast = c.readInt();
		fHeaderOnly = c.readChar();
		headerEnd = c;
	}
	
	public static TBasket getFromFile(Cursor cursor, int fBasketBytes, long fBasketEntry, long fBasketSeek) throws IOException {
		TBasket ret = new TBasket(cursor, fBasketBytes, fBasketEntry, fBasketSeek);		
		return ret;
	}
	
	private void initializePayload() {
		// FIXME mutex this eventually
		Cursor c = key.getStartCursor();
		this.payload = key.getStartCursor().getPossiblyCompressedSubcursor(key.KeyLen,
										key.Nbytes - key.KeyLen,
										key.ObjLen,
										key.KeyLen);
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
		return this.payload.readBuffer(len);
	}

}
