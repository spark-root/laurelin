package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.FileInterface;
import edu.vanderbilt.accre.laurelin.root_proxy.HadoopFile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOFactory;
import edu.vanderbilt.accre.laurelin.root_proxy.NIOFile;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;

public class IOTest {
	/*
	 * Basic meta tests
	 */
	@Test
	public void getFileImpl() throws Exception {
		assertTrue(IOFactory.openForRead("root://testfile.root") instanceof HadoopFile);
		assertTrue(IOFactory.openForRead(testfile) instanceof NIOFile);
		assertTrue(IOFactory.openForRead("./" + testfile) instanceof NIOFile);
	}

	/*
	 * Test reads from both the low-level API and the high level API
	 */
	@Test
	public void readNIOFile() throws Exception {
		int offs[] = {0, 16, 2000, 16000};
		int lens[] = {10000, 16, 20, 32};
		FileInterface file = IOFactory.openForRead(testfile);

		for (int x = 0; x < offs.length; x += 1) {
			assertArrayEquals(file.read(offs[x], lens[x]).array(), getTestBytes(offs[x], lens[x]).array());
		}
	}

	@Test
	public void readFromRootFile() throws Exception {
		int offs[] = {0, 16, 2000, 16000};
		int lens[] = {10000, 16, 20, 32};
		ROOTFile rf = ROOTFile.getInputFile(testfile);

		for (int x = 0; x < offs.length; x += 1) {
			assertArrayEquals(rf.read(offs[x], lens[x]).array(), getTestBytes(offs[x], lens[x]).array());
		}
	}

	@Test
	public void readFromRootFileCursor() throws Exception {
		long offs[] = {0, 1024, 4096};
		int lens[] = {12, 24, 1024, 96};
		ROOTFile rf = ROOTFile.getInputFile(testfile);
		Cursor cursor = rf.getCursor(0);

		assertEquals(cursor.readBuffer(100), getTestBytes(0,100));
		assertEquals(cursor.readBuffer(100), getTestBytes(100,100));
		for (int oi = 0; oi < offs.length; oi += 1) {
			cursor = rf.getCursor(offs[oi]);
			long my_off = offs[oi];
			for (int li = 0; li < lens.length; li += 1) {
				assertEquals(cursor.readBuffer(lens[li]), getTestBytes(my_off, lens[li]));
				my_off += lens[li];
			}
		}
	}

	/*
	 * Loading values from the file
	 */

	@Test
	public void compare8Bit() throws Exception {
		ROOTFile rf = ROOTFile.getInputFile(testfile2);
		Cursor c = rf.getCursor(0);
		assertEquals(255, c.readUChar(0));
		assertEquals(-1, c.readChar(0));
	}

	@Test
	public void compare16Bit() throws Exception {
		ROOTFile rf = ROOTFile.getInputFile(testfile2);
		Cursor c = rf.getCursor(0);
		assertEquals(65535, c.readUShort(0));
		assertEquals(-1, c.readShort(0));
	}

	@Test
	public void compare32Bit() throws Exception {
		ROOTFile rf = ROOTFile.getInputFile(testfile2);
		Cursor c = rf.getCursor(0);
		assertEquals(4294967295L, c.readUInt(0));
		assertEquals(-1, c.readInt(0));
	}

	@Test
	public void compare64Bit() throws Exception {
		ROOTFile rf = ROOTFile.getInputFile(testfile2);
		Cursor c = rf.getCursor(0);
		assertEquals(-1, c.readLong(0));
		// 2 ** 64 - 1
		BigInteger twoToThe64 = BigInteger.valueOf(2).pow(64);
		BigInteger twoToThe64Minus1 = twoToThe64.subtract(BigInteger.ONE);
		assertEquals(twoToThe64Minus1, c.readULong(0));
	}


	/*
	 * We'll need to make a dummy file with known contents to read.
	 */
	private final static String testfile = "ttree-unit-input";
	private final static String testfile2 = "ttree-unit-input2";

	@BeforeClass
	public static void setUp() throws Exception {
		Path path = Paths.get(testfile);
		java.nio.file.Files.deleteIfExists(path);
		Files.write(path, IOTest.getTestBytes(0, 4 * 1024 * 1024).array());
		DataOutputStream os = new DataOutputStream(new FileOutputStream(testfile2));
		os.writeInt(0xFFFFFFFF);
		os.writeInt(0xFFFFFFFF);		
		os.close();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		java.nio.file.Files.deleteIfExists(Paths.get(testfile));
		java.nio.file.Files.deleteIfExists(Paths.get(testfile2));		
	}

	private static ByteBuffer getTestBytes(long off, int len) {
		ByteBuffer buf = ByteBuffer.allocate(len);
		for (long x = off; x < off + len; x += 4) {
			buf.putInt((int)x);
		}
		return buf;
	}
}
