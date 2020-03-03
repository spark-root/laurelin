package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tukaani.xz.SeekableXZInputStream;

import edu.vanderbilt.accre.laurelin.root_proxy.io.FileInterface;

public class XZDecompressionWrapper implements FileInterface {
    private static final Logger logger = LogManager.getLogger();

    private class BackingFd extends org.tukaani.xz.SeekableInputStream {
        private RandomAccessFile fd;

        protected BackingFd(String path) throws FileNotFoundException {
            fd = new RandomAccessFile(path, "r");
        }

        @Override
        public long length() throws IOException {
            return fd.length();
        }

        @Override
        public long position() throws IOException {
            return fd.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            fd.seek(pos);
        }

        @Override
        public int read() throws IOException {
            return fd.read();
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return fd.read(b, off, len);
        }
    }

    private SeekableXZInputStream fd;
    private RandomAccessFile truthFD;

    synchronized ByteBuffer backingRead(long offset, int len) throws IOException {
        // the XZInput Stream isn't thread-safe, so synchronize all reads
        byte[] byteBuf = new byte[len];
        fd.seek(offset);
        fd.read(byteBuf, 0, len);
        if (truthFD != null) {
            byte[] truthByteBuf = new byte[len];
            truthFD.seek(offset);
            truthFD.read(truthByteBuf, 0, len);
            /*
             *  Compare the pristine with the minified. Since the minified has
             *  a lot of zero'd out bytes, we need to go one-by-one to exclude
             *  those from the comparison.
             */
            for (int i = 0; i < len; i += 1) {
                byte m = byteBuf[i];
                byte p = truthByteBuf[i];
                boolean minifiedIsZero = (m == '\0');
                boolean pristineIsZero = (p == '\0');
                boolean minifiedMatchesPristine = (m == p);
                if (!minifiedIsZero && !minifiedMatchesPristine) {
               // if ( (m != '\0') && (m != p)) {
                    String truthStr = new String();
                    String xzStr = new String();
                    final int CONTEXT = 10;
                    int j = (i > CONTEXT) ? (i - CONTEXT) : 0;
                    for (int k = j; k <= i; k += 1) {
                        truthStr += Byte.toString(truthByteBuf[k]) + " ";
                        xzStr += Byte.toString(byteBuf[k]) + " ";
                    }
                    logger.fatal("Mismatching pristine and minified reads at off " + (offset + i));
                    logger.fatal(" pristine: " + truthStr);
                    logger.fatal(" minified: " + xzStr);
                    throw new RuntimeException("Pristine and minified don't match");
                }
            }
        }
        return ByteBuffer.wrap(byteBuf);
    }

    public XZDecompressionWrapper(String path) throws IOException {
        String truthPath = path.replace("testdata/minified",  "testdata/pristine").replace(".xz", "");
        File inFile = new File(truthPath);
        if (inFile.isFile()) {
            truthFD = new RandomAccessFile(truthPath, "r");
        } else {
            truthFD = null;
        }
        fd = new SeekableXZInputStream(new BackingFd(path));
    }

    @Override
    public ByteBuffer read(long offset, long len) throws IOException {
        assert len == (int) len;
        return backingRead(offset, (int) len);
    }

    @Override
    public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException {
        return null;
    }

    @Override
    public Future<ByteBuffer> readAsync(int offset, int len) throws IOException {
        return null;
    }

    @Override
    public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        fd.close();
    }

    @Override
    public long getLimit() throws IOException {
        return fd.length();
    }

}
