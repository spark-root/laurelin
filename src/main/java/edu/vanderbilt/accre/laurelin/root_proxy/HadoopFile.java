/**
 * Constructed by IOFactory for URL pathnames (e.g root://, https://)
 */
package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class HadoopFile implements FileInterface {

	public HadoopFile(String path) {
		
	}
	/*
	 * Stubs for now to satisfy the interface 
	 */
	public ByteBuffer read(long offset, long len) throws IOException { throw new UnsupportedOperationException(); }
	public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException { throw new UnsupportedOperationException(); }
	public Future<ByteBuffer> readAsync(int offset, int len) throws IOException { throw new UnsupportedOperationException(); }
	public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException { throw new UnsupportedOperationException(); }
	public void close() throws IOException {};
	public long getLimit() throws IOException { return -1; };
}
