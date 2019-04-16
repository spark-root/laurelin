/**
 * Handles low-level loading C-struct type things and (optionally compressed)
 * byte ranges from low level I/O
 */
package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ROOTFile {
	public class FileBackedBuf implements BackingBuf {
		ROOTFile fh;
		protected FileBackedBuf(ROOTFile fh) {
			this.fh = fh; 
		}
		public ByteBuffer read(long off, int len) throws IOException {
			return fh.read(off,  len);
		}
		
		public boolean hasLimit() throws IOException {
			return true;
		}
		
		public long getLimit() throws IOException {
			return fh.getLimit();
		}
	}
	private FileInterface fh;
	/* Hide constructor */
	private ROOTFile() { }
	
	public static ROOTFile getInputFile(String path) throws IOException {
		ROOTFile rf = new ROOTFile();
		rf.fh = IOFactory.openForRead(path);
		return rf;
	}
	
	public long getLimit() throws IOException {
		return fh.getLimit();
	}
	
	/*
	 * To enable correct caching, any ByteByte buffers that get passed to
	 * users must be copies of the internal ByteBuffers we have. Otherwise
	 * we couldn't change the contents without breaking the users
	 */
	private ByteBuffer readUnsafe(long offset, int l) throws IOException {
		/*
		 * This bytebuffer can be a copy of the internal cache
		 */
		return fh.read(offset, l);
	}
	public ByteBuffer read(long offset, int len) throws IOException {
		/*
		 * TODO: 
		 * This bytebuffer must be a completely new and unlinked buffer, so
		 * copy the internal array to a new one to make sure there's nothing
		 * tying them together
		 */
		return readUnsafe(offset, len);
//		ByteBuffer ret = ByteBuffer.allocate(len);
//		ByteBuffer unsafe = readUnsafe(offset, len);
//		ret.put(unsafe.array());
//		return ret;
	}
	
	public Cursor getCursor(long off) {
		return new Cursor(new FileBackedBuf(this), off);
	}
}
