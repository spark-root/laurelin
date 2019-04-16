package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;

public class TKey {
	int Nbytes;
	int version;
	int ObjLen;
	int KeyLen;
	int Cycle;
	long fSeekKey;
	long fSeekPdir;
	String fClassName;
	String fName;
	String fTitle;
	public void getFromFile(ROOTFile fh, long off) throws IOException {
		Cursor buffer = fh.getCursor(off);
		getFromFile(buffer);
	}
	public void getFromFile(Cursor buffer) throws IOException {
		Nbytes = buffer.readInt();
		version = buffer.readShort();
		ObjLen = buffer.readInt();
		// https://root.cern.ch/doc/v614/classTDatime.html
		// This class stores the date and time with a precision of one second in an unsigned 32 bit word (950130 124559).
		long dummy = buffer.readInt();

		KeyLen = buffer.readShort();
		Cycle = buffer.readShort();
		if (version > 1000)
		{
			fSeekKey = buffer.readLong();
			fSeekPdir = buffer.readLong();
		}
		else
		{
			fSeekKey = buffer.readInt();
			fSeekPdir = buffer.readInt();    
		}

		// Get strings
		fClassName = buffer.readTString();
		fName = buffer.readTString();
		fTitle = buffer.readTString();
	}
}
