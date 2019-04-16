package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for backing buffers, which is the permanent store of the bytes
 * in the buffer. e.g. a file implementation would pass reads down to the
 * underlying file.
 */
public interface BackingBuf {
	/**
	 * Reads bytes from the backing store
	 * 
	 * @param off Offset into the backing store
	 * @param len Requested length
	 * @return A ByteBuffer with the requested bytes
	 * @throws IOException
	 */
	public ByteBuffer read(long off, int len) throws IOException;
	
	public boolean hasLimit() throws IOException;
	public long getLimit() throws IOException;
}
