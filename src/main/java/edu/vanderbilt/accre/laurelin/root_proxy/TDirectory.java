package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TDirectory {
	
	short version;
	int fNbytesKeys;
	int fNbytesName;
	
	long fSeekDir;
	long fSeekParent;
	long fSeekKeys;
	
	// the key on top of the list of subkeys
	TKey key;
	
	// all the subkeys within this directory
	List<TKey> subkeys;
	
	public TDirectory() {
		subkeys = new ArrayList<TKey>();
	}
	
	public void getFromFile(ROOTFile fh, long off) throws IOException {
		Cursor buffer = fh.getCursor(off);
		long key_top = off;
	
		version = buffer.readShort();
		int dummy = buffer.readInt(); //fDatetimeC
		dummy = buffer.readInt(); //fDatetimeM
		fNbytesKeys = buffer.readInt();
		fNbytesName = buffer.readInt();
		
		if (version > 1000)
		{
			fSeekDir = buffer.readLong();
			fSeekParent = buffer.readLong();
			fSeekKeys = buffer.readLong();
		}
		else
		{
			fSeekDir = buffer.readInt();
			fSeekParent = buffer.readInt();
			fSeekKeys = buffer.readInt();
		}
		
		// the key for the keylist entry in the file
		key = new TKey();
		key.getFromFile(fh, fSeekKeys);
		
		buffer = fh.getCursor(fSeekKeys + key.KeyLen);
		int nKeys = buffer.readInt();
		
		long position = fSeekKeys + key.KeyLen + 4;
		for (int i = 0; i < nKeys ; i += 1) {
			TKey testkey = new TKey();
			testkey.getFromFile(fh, position);
			subkeys.add(testkey);
			position += testkey.KeyLen;		
		}
	}
	
	/**
	 * Returns the TKey for the requested name
	 * @param name - Name of requested object
	 * @return TKey corresponding to object
	 */
	public TKey get(String name) {
		// FIXME cycles
		for (TKey p: subkeys) {
			if (p.fName.compareTo(name) == 0) {
				return p;
			}
		}
		return (TKey) null;
	}
}
