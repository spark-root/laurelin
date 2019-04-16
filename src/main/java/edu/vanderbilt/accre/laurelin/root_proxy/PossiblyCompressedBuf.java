package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

/**
 *  
 *
 */
public class PossiblyCompressedBuf implements BackingBuf {
	private Cursor parent;
	private long base;
	private int compressedLen;
	private int uncompressedLen;
	private Reference<ByteBuffer> decompressed;
	public PossiblyCompressedBuf(Cursor parent, long off, int compressedLen, int uncompressedLen) {
		this.parent = parent;
		this.base = off; // + parent.getBase();
		this.compressedLen = compressedLen;
		this.uncompressedLen = uncompressedLen;
		this.decompressed = new SoftReference<ByteBuffer>(null); 
	}
	
	@Override
	public ByteBuffer read(long off, int len) throws IOException {
		if (compressedLen == uncompressedLen) {
			// not compressed
			return parent.readBuffer(base + off, len);
		} else {
			ByteBuffer tmp = decompressed.get();
			if (tmp == null) {
				ByteBuffer parentBytes = parent.readBuffer(base, compressedLen);
				tmp = Compression.decompressBytes(parentBytes, compressedLen, uncompressedLen);
				decompressed = new SoftReference<ByteBuffer>(tmp);
			}
			
			// Make a copy first to prevent mutating the decompresed buffer
			ByteBuffer ret1 = tmp.duplicate();
			ret1.position((int)(off));
			ret1.limit((int)(off + len));
			ByteBuffer ret = ret1.slice();
			return ret;
		}
	}

	@Override
	public boolean hasLimit() throws IOException {
		return true;
	}

	@Override
	public long getLimit() throws IOException {
		// TODO Auto-generated method stub
		return uncompressedLen;
	}

}
