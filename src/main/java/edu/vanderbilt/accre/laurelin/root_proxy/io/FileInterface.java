package edu.vanderbilt.accre.laurelin.root_proxy.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public interface FileInterface extends AutoCloseable {
    public ByteBuffer read(long offset, long len) throws IOException;

    public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException;

    public Future<ByteBuffer> readAsync(int offset, int len) throws IOException;

    public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException;

    @Override
    public void close() throws IOException;

    public long getLimit() throws IOException;
}
