package edu.vanderbilt.accre.root_proxy;

import java.io.IOException;

public class TObject {
	short version;
	long fUniqueID;
	long fBits;
	
	void getFromCursor(Cursor cursor) throws IOException {
		// Stolen from uproot
	    version = cursor.readShort();
	    if ((version & Constants.kByteCountVMask) != 0)
	        cursor.skipBytes(4);
	   
	    fUniqueID = cursor.readUInt();
	    fBits = cursor.readUInt();
	    fBits = fBits | Constants.kIsOnHeap;
	    		
	    if ((fBits & Constants.kIsReferenced) != 0)
	        cursor.skipBytes(2);
	}
}
