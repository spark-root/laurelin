/*
 * Implements the "in memory" version of a TFile, with an optional connection
 * to a RootFile object to provide I/O
 */
package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class TFile {
	ROOTFile fh;
	private String fileName;

	// Fields in the file header
	public long fBEGIN;
	private int fCompress;
	public long fEND;
	private int fNBytesInfo;
	private int fNbytesFree;
	private int fNbytesKeys;
	private int fNbytesName;
	private long fSeekDir;
	private long fSeekFree;
	private long fSeekInfo;
	private long fSeekKeys;
	private long fSeekParent;
	private int fUnits;
	private int fVersion;
	private int nfree;

	TKey streamerKey;
	Streamer streamerInfo;
	// Toplevel TDirectory
	TDirectory directory;

	// Lists of subobjects
	List<TKey> keys;

	private TFile() {
		keys = new ArrayList<TKey>();
		directory = new TDirectory();
	}

	static public TFile getFromFile(String path) throws IOException {
		TFile ret = new TFile();
		ret.openForRead(path);
		return ret;
	}

	public void openForRead(String path) throws IOException {
	    fileName = path;
		fh = ROOTFile.getInputFile(path);
		Cursor c = fh.getCursor(0);
		parseHeaderImpl(false);
		if (fVersion > 1000000) {
			parseHeaderImpl(true);
		}

		/*
		 * Load the list of keys in the file (not sure if this is needed long-term)
		 */
		long position = fBEGIN;
		while (position < fEND) {
			int keyBytes = c.readInt(position);
			if (keyBytes < 0) {
				// Negative nBytes are free blocks
				position += (-keyBytes);
				continue;
			}
			TKey testkey = new TKey();
			testkey.getFromFile(fh, position);
			keys.add(testkey);
			position += testkey.Nbytes;
		}
		/*
		 * We don't need/want the TNamed stuff for now, which is why we're pushing
		 * the inputs by fNbytesName bytes
		 */

		/*
		 * There is a TDirectory at position fBEGIN+fNbytesName
		 * https://github.com/scikit-hep/uproot/blob/662d1f859f8ba7a5d908a249b3cae5b743e56a19/uproot/rootio.py#L193
		 */
		directory.getFromFile(fh, fBEGIN + fNbytesName);

		/*
		 * And then the Streamers can be found at fSeekInfo
		 */
		streamerKey = new TKey();
		streamerKey.getFromFile(fh, fSeekInfo);
		Cursor fhCursor = fh.getCursor(0);
		Cursor streamerCursor =
				fhCursor.getPossiblyCompressedSubcursor(
						fSeekInfo + streamerKey.KeyLen,
						streamerKey.Nbytes - streamerKey.KeyLen,
						streamerKey.ObjLen,
						streamerKey.KeyLen);
		streamerInfo = new Streamer();
		streamerInfo.getFromCursor(streamerCursor, 0);

	}

	private void parseHeaderImpl(boolean largeFile) throws IOException {
		Cursor buffer = fh.getCursor(0);

		String magic = "root";
		ByteBuffer file_magic = buffer.readBuffer(4);
		String head_str = new String(file_magic.array(), "US-ASCII");

		if (!head_str.equals(magic)) {
			throw new IOException("Not a ROOT file (" + head_str + ") not (" + magic + ")");
		}

		// ROOT has a different file format if the file is > 2GB.
		// It signals this by changing the version number
		fVersion = buffer.readInt();
		fVersion %= 1000000;
		fBEGIN = buffer.readInt();
		if (largeFile)
		{
			fEND = buffer.readLong();
			fSeekFree = buffer.readLong();
			fNbytesFree = buffer.readInt();
			nfree = buffer.readInt();
			fNbytesName = buffer.readInt();
			fUnits = buffer.readChar();
			fCompress = buffer.readInt();
			fSeekInfo = buffer.readLong();
			fNBytesInfo = buffer.readInt();
		}
		else
		{
			fEND = buffer.readInt();
			fSeekFree = buffer.readInt();
			fNbytesFree = buffer.readInt();
			nfree = buffer.readInt();
			fNbytesName = buffer.readInt();
			fUnits = buffer.readChar();
			fCompress = buffer.readInt();
			fSeekInfo = buffer.readInt();
			fNBytesInfo = buffer.readInt();
		}
	}

	public Proxy getProxy(String name) throws IOException {
		// Handle subdirectories .. later
		TKey key = directory.get(name);
		if (key == null) {
			throw new NoSuchElementException("Could not load \"" + name + "\"");
		}
		Cursor keyCursor = fh.getCursor(key.fSeekKey);
		Cursor valCursor = keyCursor.getPossiblyCompressedSubcursor(key.KeyLen,
				key.Nbytes - key.KeyLen,
				key.ObjLen,
				key.KeyLen);
		return streamerInfo.deserializeWithStreamer(key, valCursor);
	}

	public Cursor getCursorAt(long off) throws IOException {
		return fh.getCursor(off);
	}

	public String getFileName() {
	    return fileName;
	}
}
