package edu.vanderbilt.accre.laurelin.root_proxy.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for backing buffers, which is the permanent store of the bytes in
 * the buffer. e.g. a file implementation would pass reads down to the
 * underlying file.
 */
public interface BackingBuf {
    /**
     * Reads bytes from the backing store
     *
     * @param off Offset into the backing store
     * @param len Requested length
     * @return A ByteBuffer with the requested bytes
     * @throws IOException Exception if backing store fails
     */
    public ByteBuffer read(long off, long len) throws IOException;

    public boolean hasLimit() throws IOException;

    public long getLimit() throws IOException;

    public BackingBuf duplicate();
}
