/**
 * Constructed by IOFactory for non-URL pathnames (e.g. local files)
 */

package edu.vanderbilt.accre.root_proxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Future;

public class NIOFile implements FileInterface {
	private RandomAccessFile fh;
	private FileChannel channel;
	public NIOFile(String path) throws FileNotFoundException {
		this.fh = new RandomAccessFile(path, "r");
	    this.channel = fh.getChannel();
	}
	public ByteBuffer read(long offset, int len) throws IOException {
		ByteBuffer ret = ByteBuffer.allocate(len);
		if (this.channel.read(ret, offset) != len) {
			throw new IOException("Short read");
		}
		return ret;
	}
	public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException {
		throw new UnsupportedOperationException();
	}
	public Future<ByteBuffer> readAsync(int offset, int len) throws IOException { 
		throw new UnsupportedOperationException();
	}
	public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException {
		throw new UnsupportedOperationException();
	}
	public void close() throws IOException {
		channel.close();
		fh.close();
	}

	public long getLimit() throws IOException {
		return fh.length();
	}
}
