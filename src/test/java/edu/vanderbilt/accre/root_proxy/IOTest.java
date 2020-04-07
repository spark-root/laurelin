package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

    // Needed to instantiate hadoop filesystems
    Configuration hadoopConf = new Configuration();

    /*
     * Basic meta tests
     */
    @Test
    public void getFileImpl() throws Exception {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        assertTrue(IOFactory.openForRead("file:///" + s + "/" + testfile) instanceof HadoopFile);
        assertTrue(IOFactory.openForRead(testfile) instanceof NIOFile);
        assertTrue(IOFactory.openForRead("./" + testfile) instanceof NIOFile);
    }

    /*
     * Test reads from both the low-level API and the high level API
     */
    @Test
    public void readNIOFile() throws Exception {
        int[] offs = {0, 16, 2000, 16000};
        int[] lens = {10000, 16, 20, 32};
        FileInterface file = IOFactory.openForRead(testfile);

        for (int x = 0; x < offs.length; x += 1) {
            assertArrayEquals(file.read(offs[x], lens[x]).array(), getTestBytes(offs[x], lens[x]).array());
        }
    }

    @Test
    public void readHadoopFile() throws Exception {
        int[] offs = {0, 16, 2000, 16000};
        int[] lens = {10000, 16, 20, 32};
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        FileInterface file = IOFactory.openForRead("file:///" + s + "/" + testfile);

        for (int x = 0; x < offs.length; x += 1) {
            assertArrayEquals(file.read(offs[x], lens[x]).array(), getTestBytes(offs[x], lens[x]).array());
        }
    }

    @Test
    public void readFromRootFile() throws Exception {
        int[] offs = {0, 16, 2000, 16000};
        int[] lens = {10000, 16, 20, 32};
        ROOTFile rf = ROOTFile.getInputFile(testfile);

        for (int x = 0; x < offs.length; x += 1) {
            byte[] expected = new byte[lens[x]];
            byte[] actual = new byte[lens[x]];
            rf.read(offs[x], lens[x]).get(actual, 0, lens[x]);
            getTestBytes(offs[x], lens[x]).get(expected, 0, lens[x]);
            assertArrayEquals(expected, actual);
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
    private static final String testfile = "ttree-unit-input";
    private static final String testfile2 = "ttree-unit-input2";

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
        buf.flip();
        buf.position(0);
        buf.limit(len);
        return buf;
    }

    /*
     * Testing file listing
     */
    @Test
    public void searchDirectory_nio() throws IOException {
        List<org.apache.hadoop.fs.Path> files = resolveHelper("testdata/recursive");
        assertEquals(3, files.size());
    }

    @Test
    public void searchDirectory_hadoop() throws IOException {
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        List<org.apache.hadoop.fs.Path> files = resolveHelper("file:///" + s + "/" + "testdata/recursive");
        assertEquals(3, files.size());
    }

    // Save some keystrokes
    private List<org.apache.hadoop.fs.Path> resolveHelper(String ...args) throws IOException {
        List<org.apache.hadoop.fs.Path> ret = IOFactory.resolvePathList(new ArrayList<String>(Arrays.asList(args)));
        Collections.sort(ret);
        return ret;
    }

    // Just check the suffixes, since different devs will have different working
    // dirs and it's kinda a PITA to jump through the hoops to convert it all
    private void assertPathListsSame(String message, String[] expected, List<org.apache.hadoop.fs.Path> actual) throws IOException {
        FileSystem fs = FileSystem.getLocal(hadoopConf);
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(message, actual.get(i).toString().endsWith(expected[i]));
        }
        assertEquals(message + "Number of elements should match", expected.length, actual.size());
    }

    String[] allGlobFiles = new String[] { "testdata/globtest/1/1/1_1_1.root",
            "testdata/globtest/1/1/1_1_2.root",
            "testdata/globtest/1/1/1_1_3.root",
            "testdata/globtest/1/2/1_2_1.root",
            "testdata/globtest/1/2/1_2_2.root",
            "testdata/globtest/1/2/1_2_3.root",
            "testdata/globtest/1/3/1_3_1.root",
            "testdata/globtest/1/3/1_3_2.root",
            "testdata/globtest/1/3/1_3_3.root",
            "testdata/globtest/2/1/2_1_1.root",
            "testdata/globtest/2/1/2_1_2.root",
            "testdata/globtest/2/1/2_1_3.root",
            "testdata/globtest/2/2/2_2_1.root",
            "testdata/globtest/2/2/2_2_2.root",
            "testdata/globtest/2/2/2_2_3.root",
            "testdata/globtest/2/3/2_3_1.root",
            "testdata/globtest/2/3/2_3_2.root",
            "testdata/globtest/2/3/2_3_3.root",
            "testdata/globtest/3/1/3_1_1.root",
            "testdata/globtest/3/1/3_1_2.root",
            "testdata/globtest/3/1/3_1_3.root",
            "testdata/globtest/3/2/3_2_1.root",
            "testdata/globtest/3/2/3_2_2.root",
            "testdata/globtest/3/2/3_2_3.root",
            "testdata/globtest/3/3/3_3_1.root",
            "testdata/globtest/3/3/3_3_2.root",
            "testdata/globtest/3/3/3_3_3.root"};

    @Test
    public void resolvePathList_recursion_full() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest");
        assertPathListsSame("recursion_full", allGlobFiles, paths);
    }

    @Test
    public void resolvePathList_recursion_full_duplicated() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest", "testdata/globtest");
        assertPathListsSame("recursion_full_dup", allGlobFiles, paths);
    }

    @Test
    public void resolvePathList_recursion_globbed() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/{1,2,3}");
        assertPathListsSame("recursion_globbed", allGlobFiles, paths);
    }

    String[] someGlobFiles = new String[] {
            "testdata/globtest/1/2/1_2_1.root",
            "testdata/globtest/1/2/1_2_2.root",
            "testdata/globtest/1/2/1_2_3.root",
            "testdata/globtest/1/3/1_3_1.root",
            "testdata/globtest/1/3/1_3_2.root",
            "testdata/globtest/1/3/1_3_3.root",
    };

    @Test
    public void resolvePathList_recursion_globbed2() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/{1{/2,/3}}");
        assertPathListsSame("recursion_globbed2", someGlobFiles, paths);
    }

    @Test
    public void resolvePathList_recursion_notglobbed() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/1/2", "testdata/globtest/1/3");
        assertPathListsSame("recursion_notglobbed", someGlobFiles, paths);
    }

    @Test
    public void resolvePathList_explicitlist() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper(someGlobFiles);
        assertPathListsSame("explicit_list", someGlobFiles, paths);
    }

    @Test
    public void resolvePathList_explicit_one() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/1/2/1_2_1.root");
        assertPathListsSame("explicit_one", new String[] { "testdata/globtest/1/2/1_2_1.root" }, paths);
    }

    @Test(expected = IOException.class)
    public void resolvePathList_badglob() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/nonexistent/*");
    }

    @Test(expected = IOException.class)
    public void resolvePathList_badglob_withothers() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/nonexistent/*", "testdata/globtest/1/3");
    }

    @Test(expected = IOException.class)
    public void resolvePathList_badpath() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/nonexistent/");
    }

    @Test(expected = IOException.class)
    public void resolvePathList_badpath_withothers() throws IOException {
        List<org.apache.hadoop.fs.Path> paths = resolveHelper("testdata/globtest/nonexistent/", "testdata/globtest/1/3");
    }


}
