/**
 * Constructed by IOFactory for URL pathnames (e.g root://, https://)
 */

package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFile implements FileInterface {
    FSDataInputStream fd;
    long limit;

    public HadoopFile(String pathStr) throws IOException {
        Configuration conf = new Configuration();
	URI uri = URI.create(pathStr);
        FileSystem fileSystem = FileSystem.get(uri, conf);
        Path path = new Path(uri);
        fd = fileSystem.open(path, 'r');
        limit = fileSystem.getFileStatus(path).getLen();
    }

    /*
     * Stubs for now to satisfy the interface
     */
    @Override
    public ByteBuffer read(long offset, long len) throws IOException {
        if (len != ((int) len)) {
            throw new RuntimeException("Cannot perform a single read > 2GB");
        }
        ByteBuffer ret = ByteBuffer.allocate((int)len);
        fd.read(offset, ret.array(), 0, (int)len);
        return ret;
    }

    @Override
    public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<ByteBuffer> readAsync(int offset, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        fd.close();
    }

    @Override
    public long getLimit() throws IOException {
        return limit;
    }
}