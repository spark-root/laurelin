package edu.vanderbilt.accre.laurelin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

public class Minify {
    public static void main(String[] args) throws FileNotFoundException, IOException {
        String profilePath = "unittest_ioprofile.txt";
        HashMap<String, RangeSet<Long>> rangeMap = new HashMap<String, RangeSet<Long>>();
        boolean eofFound = false;
        try (BufferedReader br = new BufferedReader(new FileReader(profilePath))) {
            String line = null;
            String currPath = null;
            RangeSet<Long> currRange = null;
            while ((line = br.readLine()) != null) {
                if (eofFound) {
                    throw new RuntimeException("Found EOF at not the end of the file");
                } else if (line.startsWith("#BEGIN")) {
                    String[] sl = line.split(" ");
                    System.out.println("Loading profile for " + sl[1]);
                    currPath = sl[1];
                    currRange = TreeRangeSet.create();
                    rangeMap.put(currPath, currRange);
                } else if (line.startsWith("#EOF")) {
                    eofFound = true;
                } else {
                    String[] sl = line.split(" ");
                    long lowerOffset = Long.parseLong(sl[0]);
                    long upperOffset = Long.parseLong(sl[1]);
                    currRange.add(Range.closedOpen(lowerOffset, upperOffset));
                }
            }
        }

        /*
         * How large of an XZ block do we want to compress. This becomes the
         * granularity level of the random-access decompression on the other
         * side
         */
        final int BLOCK_SIZE = 10 * 1024 * 1024;

        /*
         * Read the decompressed bytes from pristine, and write minified bytes
         * out to xz's compression layer, making a new block every BLOCK_SIZE
         * bytes
         */
        final long ZEROBUF_SIZE = 10 * 1024 * 1024;
        byte[] zeroBuf = new byte[(int) (ZEROBUF_SIZE)];
        for (String inPath: rangeMap.keySet()) {
            String outPath = inPath.replace("testdata/pristine", "testdata/minified") + ".xz";
            // Read the decompressed bytes in
            System.out.println("Compressing " + outPath);
            long totalFileBytes = 0;
            long fileBlockCount = 0;

            try (FileInputStream inFd = new FileInputStream(inPath);
                    XZOutputStream outxz = new XZOutputStream(new FileOutputStream(outPath), new LZMA2Options(0));
                    FileOutputStream compareFd = new FileOutputStream(outPath + ".real")) {
                LinkedList<Range<Long>> tmpList = new LinkedList<Range<Long>>();
                tmpList.addAll(rangeMap.get(inPath).asDescendingSetOfRanges());
                Collections.reverse(tmpList);
                long blockBegin = 0;
                long lastWritePos = 0;
                long startTime = System.currentTimeMillis();
                long currTime = startTime;

                // track positions on our own to ensure we're writing properly
                long inPos = 0;
                for (Range<Long> r: tmpList) {
                    long upper = r.upperEndpoint();
                    long lower = r.lowerEndpoint();
                    long tmpLen = upper - lower;
                    int len = (int) (upper - lower);
                    assert len == tmpLen;
                    byte[] buf = new byte[len];
                    inPos += inFd.skip(lower - inPos);
                    assert inPos == lower;
                    int bytesRead = inFd.read(buf, 0, len);
                    assert bytesRead == len;
                    inPos += bytesRead;
                    assert inPos == upper;
                    /*
                     *  Write a bunch of zeros between the last position and
                     *  here. Zeros should compress easy, right?
                     */
                    long zerosNeeded = lower - lastWritePos;
                    assert zerosNeeded == (int)zerosNeeded;
                    if (zerosNeeded < ZEROBUF_SIZE) {
                        outxz.write(zeroBuf, 0, (int) zerosNeeded);
                        compareFd.write(zeroBuf, 0, (int) zerosNeeded);
                    } else {
                        outxz.write(new byte[(int) zerosNeeded], 0, (int) zerosNeeded);
                        compareFd.write(new byte[(int) zerosNeeded], 0, (int) zerosNeeded);
                    }

                    // Now write the actual data we want
                    outxz.write(buf, 0, len);
                    compareFd.write(buf, 0, len);
                    lastWritePos = upper;
                    totalFileBytes += len;
                    if ((System.currentTimeMillis() - currTime) > 10 * 1000) {
                        System.out.println(" compressed " + totalFileBytes + " bytes (" + 1000 * totalFileBytes / (System.currentTimeMillis() - startTime) + " B/sec)");
                        currTime = System.currentTimeMillis();
                    }
                    if ((upper - blockBegin) > BLOCK_SIZE) {
                        outxz.endBlock();
                        fileBlockCount += 1;
                        blockBegin = upper;
                    }
                }
                /*
                 * Pad the end of the file with zeros to the decompressed file
                 * is the right length
                 */
                File inFile = new File(inPath);
                int paddingZeros = (int) (inFile.length() - lastWritePos);
                byte[] paddingBuf = new byte[paddingZeros];
                outxz.write(paddingBuf, 0, paddingZeros);;
                compareFd.write(paddingBuf, 0, paddingZeros);
            }
            System.out.println("Compressed " + totalFileBytes + " into " + fileBlockCount + " blocks");
        }
    }

}
