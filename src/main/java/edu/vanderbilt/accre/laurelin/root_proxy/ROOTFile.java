/**
 * Handles low-level loading C-struct type things and (optionally compressed)
 * byte ranges from low level I/O
 */
package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.FileProfiler;

public class ROOTFile {
    private static final Logger logger = LogManager.getLogger();

    public static class FileBackedBuf implements BackingBuf {
        ROOTFile fh;

        /**
         * Size of a "cache page"
         */
        private static final int CACHE_PAGE_SIZE = 16 * 1024;

        /**
         * Total number of caches pages to store. Past this, the cache library
         * will begin to evict unneeded pages
         */
        private static final int CACHE_PAGE_COUNT = 1024;

        /**
         * Maximum size we'll attempt to cache in a single read. Past that,
         * we'll just pass it directly since more often than not, it's some
         * sort of compressed blob
         */
        private static final int CACHE_READ_MAX = 4 * CACHE_PAGE_SIZE;

        public class CacheKey {
            public ROOTFile fh;
            public long off;

            public CacheKey(ROOTFile fh, long off) {
                this.fh = fh;
                this.off = off;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                return (((fh == ((CacheKey) obj).fh))
                        && (off == ((CacheKey) obj).off));
            }

            @Override
            public int hashCode() {
                return (int) (fh.hashCode() + (off / (2 * 1024 * 1024)));
            }

        }

        static LoadingCache<CacheKey, ByteBuffer> cache;

        static {
            CacheLoader<CacheKey, ByteBuffer> loader = new CacheLoader<CacheKey, ByteBuffer>() {
                    // FIXME - implement loadAll to get many pages at once
                    @Override
                    public ByteBuffer load(CacheKey key) throws Exception {
                        if (key.fh.getLimit() > key.off + CACHE_PAGE_SIZE) {
                            return key.fh.read(key.off, CACHE_PAGE_SIZE);
                        } else {
                            long shortCount = key.fh.getLimit() - key.off;
                            return key.fh.read(key.off, shortCount);
                        }
                    }
                };
            cache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_PAGE_COUNT)
                .build(loader);
        }

        protected FileBackedBuf(ROOTFile fh) {
            this.fh = fh;

        }

        /*
         * All application-level reads enter the I/O subsystem here
         */
        @Override
        public ByteBuffer read(long off, long len) throws IOException {
            ByteBuffer ret;
            long lowerPage = off / CACHE_PAGE_SIZE;
            long upperPage = (off + len) / CACHE_PAGE_SIZE;
            try (Event ev = this.fh.profile.startUpperOp(off, (int)len)) {
                if ((len > CACHE_READ_MAX)
                        || (lowerPage != upperPage)) {
                    /*
                     *  1) Don't cache very large reads, since they will end up
                     *     being compressed baskets more often than not (and
                     *     the decompressed versions are what's stored)
                     *  2) Shortcut out if the read would otherwise straddle
                     *     a cache to make the initial code simpler
                     */
                    ret = fh.read(off, len);
                } else {
                    try {
                        ret = cache.get(new CacheKey(fh, lowerPage * CACHE_PAGE_SIZE)).duplicate();
                        long newPos = (off - (CACHE_PAGE_SIZE * lowerPage));
                        ret.position((int) newPos);
                        ret.limit((int)(newPos + len));
                        ret = ret.slice();
                    } catch (ExecutionException e) {
                        throw new IOException(e);
                    }
                }
            }  catch (Exception e) {
                throw new IOException(e);
            }
            return ret;
        }

        @Override
        public boolean hasLimit() throws IOException {
            return true;
        }

        @Override
        public long getLimit() throws IOException {
            return fh.getLimit();
        }

        @Override
        public BackingBuf duplicate() {
            return new FileBackedBuf(fh);
        }
    }

    private FileInterface fh;
    protected FileProfiler profile;

    /* Hide constructor */
    private ROOTFile(String path) {
        profile = IOProfile.getInstance().beginProfile(path);
    }

    public static ROOTFile getInputFile(String path) throws IOException {
        ROOTFile rf = new ROOTFile(path);
        rf.fh = IOFactory.openForRead(path);
        return rf;
    }

    public long getLimit() throws IOException {
        return fh.getLimit();
    }

    /*
     * To enable correct caching, any ByteByte buffers that get passed to
     * users must be copies of the internal ByteBuffers we have. Otherwise
     * we couldn't change the contents without breaking the users
     */
    private ByteBuffer readUnsafe(long offset, long l) throws IOException {
        /*
         * This bytebuffer can be a copy of the internal cache
         */
        ByteBuffer ret;
        try (Event time = profile.startLowerOp(offset, (int)l)) {
            // This is a call to the actual filesystem
            ret = fh.read(offset, l);
            ret.position(0);
            ret = ret.asReadOnlyBuffer();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return ret;
    }

    public ByteBuffer read(long offset, long len) throws IOException {
        /*
         * TODO:
         * This bytebuffer must be a completely new and unlinked buffer, so
         * copy the internal array to a new one to make sure there's nothing
         * tying them together
         */
        return readUnsafe(offset, len);
    }

    public Cursor getCursor(long off) {
        return new Cursor(new FileBackedBuf(this), off);
    }
}
