package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;

import org.junit.Test;

import com.google.common.base.Ticker;

import edu.vanderbilt.accre.laurelin.root_proxy.FileInterface;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFileCache;

public class TestROOTFileCache {
    private static class TestTicker extends Ticker {
        long val = 0;

        @Override
        public long read() {
            return val;
        }

        public void write(long val) {
            this.val = val;
        }
    }

    @Test
    public void testOpenSame() throws IOException {
        TestTicker testTicker = new TestTicker();

        // Empty caches should be empty
        ROOTFileCache cache = new ROOTFileCache(testTicker);
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // Load a file and make sure we don't get any new references
        ROOTFile ret = cache.getROOTFile("testdata/stdvector.root");
        assertEquals(1, cache.getOpenCount());
        assertEquals(1, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // ... even after running GC
        System.gc();
        cache.incrementalCleanup();
        assertEquals(1, cache.getOpenCount());
        assertEquals(1, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // Load a 2nd file, and make sure it's the same file
        ROOTFile ret2 = cache.getROOTFile("testdata/stdvector.root");
        assertSame(ret, ret2);
        System.gc();
        cache.incrementalCleanup();
        assertEquals(1, cache.getOpenCount());
        assertEquals(1, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // Let one file go out of scope, the cache should be unchanged
        ret = null;
        System.gc();
        cache.incrementalCleanup();
        assertEquals(1, cache.getOpenCount());
        assertEquals(1, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // Let the last file go out of scope
        FileInterface oldFD = ret2.getFileInterface();
        ret2 = null;
        cache.forciblyInvalidateOpenFile("testdata/stdvector.root");
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(1, cache.getTimedCloseCount());

        // Make sure nothing happens since we're not incrementing the Ticker
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(1, cache.getTimedCloseCount());

        // Now revive the file
        ret2 = cache.getROOTFile("testdata/stdvector.root");
        System.gc();
        cache.incrementalCleanup();
        assertSame(oldFD, ret2.getFileInterface());
        assertEquals(1, cache.getOpenCount());
        assertEquals(1, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

        // OK, rekill it
        ret2 = null;
        cache.forciblyInvalidateOpenFile("testdata/stdvector.root");
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(1, cache.getTimedCloseCount());

        // Make sure nothing happens since we're not incrementing the Ticker
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(1, cache.getTimedCloseCount());

        // Say it's 30 secs in the future and ensure the refs go away
        // (the timeout is a minute)
        long sec_to_nsec = 1000000000;
        testTicker.write(30 * sec_to_nsec);
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(1, cache.getTimedCloseCount());

        // Say it's a year in the future and ensure the refs go away
        testTicker.write(365 * 24 * 60 * 60 * sec_to_nsec);
        System.gc();
        cache.incrementalCleanup();
        assertEquals(0, cache.getOpenCount());
        assertEquals(0, cache.getPhantomReferenceCount());
        assertEquals(0, cache.getTimedCloseCount());

    }

}
