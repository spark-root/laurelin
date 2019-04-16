package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;

/**
 * used to parse/verify we read the appropriate amount of bytes
 * from the buffer for an object
 */
public class RangeCheck {
	private long start;
	private long count;
	private int vers;
	public int getVers() { return vers; }
	public long getStart() { return start; }
	public long getCount() { return count; }

	public RangeCheck(Cursor cursor) throws IOException {
		start = cursor.getOffset();
		count = cursor.readInt();
		vers = cursor.readUShort();
		if ( (((int)count) & Constants.kByteCountMask) != 0 ) {
			count = ((int)count) & ~Constants.kByteCountMask;
			count += 4;
		} else {
			cursor.setOffset(start);
			vers = cursor.readUShort();
			count = -1;
		}
	}
	
	public void verify(Cursor cursor) throws IOException {
		if (count != -1) {
			long observed = cursor.getOffset() - start;
			if (observed != count) {
				throw new IOException("Read an inappropriate number (" + observed + ") of bytes. Expected " + count);
			}
		}
	}
}
