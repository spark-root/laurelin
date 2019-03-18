package edu.vanderbilt.accre.root_proxy;

import java.io.IOException;

public class TList {
	private TObject base;
	private String fName;
	private int fSize;
	private Cursor payload;
	
	public void readFromCursor(Cursor cursor) throws IOException {
		base = new TObject();
		cursor.startCheck();
		base.getFromCursor(cursor);
		fName = cursor.readTString();
		fSize = cursor.readInt();
		payload = cursor.duplicate();
	}
	
	public Cursor getDataCursor() { return payload.duplicate(); }
	public int size() { return fSize; }
}
