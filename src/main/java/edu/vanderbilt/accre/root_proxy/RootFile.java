/**
 * Handles low-level loading C-struct type things and (optionally compressed)
 * byte ranges from low level I/O
 */
package edu.vanderbilt.accre.root_proxy;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class RootFile {
	private FileInterface fh;
	/* Hide constructor */
	private RootFile() { }
	
	public static RootFile getInputFile(String path) throws Exception {
		RootFile rf = new RootFile();
		rf.fh = IOFactory.openForRead(path);
		return rf;
	}
	
	/*
	 * To enable correct caching, any ByteByte buffers that get passed to
	 * users must be copies of the internal ByteBuffers we have. Otherwise
	 * we couldn't change the contents without breaking the users
	 */
	private ByteBuffer readUnsafe(long offset, int len) throws IOException {
		/*
		 * This bytebuffer can be a copy of the internal cache
		 */
		return fh.read(offset, len);
	}
	public ByteBuffer read(long offset, int len) throws IOException {
		/*
		 * This bytebuffer must be a completely new and unlinked buffer, so
		 * copy the internal array to a new one to make sure there's nothing
		 * tying them together
		 */
		ByteBuffer ret = ByteBuffer.allocate(len);
		ByteBuffer unsafe = readUnsafe(offset, len);
		ret.put(unsafe.array());
		return ret;
	}
	
	/*
	 * Map C types to Java types
	 */
	public short readU8(long offset) throws IOException {
		short ret = readUnsafe(offset, 1).get(0);
		if (ret < 0)
			ret += 256;
		return ret;
	}
	
	public byte readS8(long offset) throws IOException {
		return readUnsafe(offset, 1).get(0);
	}
	
	public int readU16(long offset) throws IOException {
		int ret = readUnsafe(offset, 2).getShort(0);
		if (ret < 0)
			ret += 65536L;
		return ret;
	}
	
	public short readS16(long offset) throws IOException {
		return readUnsafe(offset, 4).getShort(0);
	}
	public long readU32(long offset) throws IOException {
		long ret = readUnsafe(offset, 4).getInt(0);
		if (ret < 0)
			ret += 4294967296L;
		return ret;
	}
	
	public int readS32(long offset) throws IOException {
		return readUnsafe(offset, 4).getInt(0);
	}
	
	public BigInteger readU64(long offset) throws IOException {		
		BigInteger ret = new BigInteger(readUnsafe(offset, 8).array());
		if (ret.compareTo(BigInteger.ZERO) == -1) {
			ret = ret.add(BigInteger.valueOf(2).pow(64));
		}
		return ret;
	}
	
	public long readS64(long offset) throws IOException {
		return readUnsafe(offset, 8).getLong(0);
	}
	
	/*
	 * A Cursor is an object that tracks the current read location, so multiple
	 * threads can access the file at once without stomping their positions
	 */
	public Cursor getCursor() {
		return new Cursor(this);
	}
	public Cursor getCursor(long off) {
		return new Cursor(this, off);
	}
	class Cursor {
		private RootFile rf;
		private long off = 0;
		protected Cursor(RootFile rf) {
			this.rf = rf;
			off = 0;
		}
		protected Cursor(RootFile rf, long offset) {
			this.rf = rf;
			off = offset;
		}
		public ByteBuffer readBuffer(long offset, int len) throws IOException {
			return rf.readUnsafe(offset, len);
		}
		public ByteBuffer readBuffer(int len) throws IOException {
			ByteBuffer ret = rf.readUnsafe(off, len);
			off += ret.limit();
			return ret;
		}
		
	}
}
