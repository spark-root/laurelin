/**
 *
 */
package edu.vanderbilt.accre.laurelin.root_proxy.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author meloam
 *
 */
public class InMemoryBackingBuf implements BackingBuf {
	private ByteBuffer buf;

	public InMemoryBackingBuf(ByteBuffer buf) {
		this.buf = buf;
	}

	@Override
	public ByteBuffer read(long off, long len) throws IOException {
		ByteBuffer ret = buf.slice();
		ret.position((int) off);
		ret.limit((int) len);
		return ret;
	}

	@Override
	public boolean hasLimit() throws IOException {
		return true;
	}

	@Override
	public long getLimit() throws IOException {
		return buf.limit();
	}

	@Override
	public BackingBuf duplicate() {
		return new InMemoryBackingBuf(buf);
	}

}
