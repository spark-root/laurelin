package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Ticker;

/**
 * Tracks the lifetime of ROOTFiles and their underlying FileInterface
 * references
 *
 * <p>See docs/filehandle-refcounting.md for design info
 *
 */
public class ROOTFileCache {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Holds weak references to ROOTFile objects -- enforces a one-to-one
     * mapping from a path to a given ROOTFile
     */
    private HashMap<String, WeakReference<ROOTFile>> fileCache = new HashMap<String, WeakReference<ROOTFile>>();


    /**
     * Notification queue for when ROOTFiles are unreachable
     */
    private ReferenceQueue<ROOTFile> phantomQueue = new ReferenceQueue<ROOTFile>();

    /**
     * Hold onto our phantom references so they themselves don't get GCd
     */
    private HashMap<String, ROOTFileFinalizer> phantomMap = new HashMap<String, ROOTFileFinalizer>();

    /**
     * Hold onto our FileInterfaces so they themselves don't get GCd
     */
    private HashMap<String, FileInterface> interfaceMap = new HashMap<String, FileInterface>();

    /**
     * Lock used to prevent races
     */
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Holds references to unused FileInterface objects. These are able to be
     * reaped after the end of the timeout.
     */
    private TimedCache timedCache;

    /**
     * Default constructor
     */
    public ROOTFileCache() {
        this(Ticker.systemTicker());
    }

    /**
     * Constructor which accepts a Ticker object, used for testing.
     * @param ticker Time source to use for this ROOTFileCache
     */
    public ROOTFileCache(Ticker ticker) {
        timedCache = new TimedCache(phantomMap, ticker);
    }

    /**
     * Instead of using a background thread, cleanup tasks are manually
     * executed from time to time.
     */
    public void incrementalCleanup() {
        try {
            lock.lock();

            // First, release stale ROOTFile references in fileCache
            LinkedList<String> staleRefs = new LinkedList<String>();
            for (String key: fileCache.keySet()) {
                if (fileCache.get(key).get() == null) {
                    staleRefs.add(key);
                }
            }
            for (String key: staleRefs) {
                fileCache.remove(key);
            }

            /*
             * Next, resolve any stale PhantomReferences to ROOTFiles. The
             * referents have already been finalize()d by the JVM and have no
             * other references pointing to them (except for other PhantomRefs)
             */
            ROOTFileFinalizer ref;
            while ((ref = (ROOTFileFinalizer) phantomQueue.poll()) != null) {
                /*
                 *  A ROOTFile is gone, let's queue the underlying FileInterface
                 *  for deletion
                 */

                /*
                 *  There should be no way a file can exist in the file cache
                 *  and the timedCache at the same time
                 */
                logger.trace("Finalizing " + ref.getPath() + " " + ref.getFile());
                timedCache.insert(ref.getPath(), ref.getFile());
                phantomMap.remove(ref.getPath());
                fileCache.remove(ref.getPath());
                interfaceMap.remove(ref.getPath());
                ref.clear();
            }

            /*
             * Then, remove FileInterface values that expired from the
             * timedCache. This will trigger a close() to the underlying file.
             * We remove the final reference which is held by the interfaceMap
             * which will additionally allow the GC to reap the object itself.
             */
            for (String key: timedCache.cleanUp()) {
                interfaceMap.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Forcibly say the ROOTFile went out of scope for testing.
     * It's difficult to unittest this class because much of its behavior
     * depends on the GC implementation (which can vary based on platform). This
     * function triggers the same callbacks that the JVM would call to let us
     * reliably test that functionality.
     * @param path Path of the ROOTFile to invalidate
     */
    public void forciblyInvalidateOpenFile(String path) {
        try {
            lock.lock();
            fileCache.get(path).clear();
            phantomMap.get(path).enqueue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Forcibly say the ROOTFile went out of scope for testing.
     * It's difficult to unittest this class because much of its behavior
     * depends on the GC implementation (which can vary based on platform). This
     * function triggers the same callbacks that the JVM would call to let us
     * reliably test that functionality.
     * @param path Path of the ROOTFile to invalidate
     */
    public void forciblyInvalidateOpenFile(String path) {
        try {
            lock.lock();
            fileCache.invalidate(path);
            phantomMap.get(path).enqueue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets a ROOTFile pointing to the given URI, ensuring that they're unique
     * within the cache object and with a timeout to prevent immediately
     * closing the file when it could be opened shortly afterwards.
     * @param path URI of file to open
     * @return The ROOTFile representing the path
     * @throws IOException - Thrown if there are I/O errors
     */
    public ROOTFile getROOTFile(String path) throws IOException {
        try {
            lock.lock();
            logger.trace(String.format("Acquiring (%s): %s", this, path));
            WeakReference<ROOTFile> ref = fileCache.get(path);
            ROOTFile ret = null;
            // Do it in this order to keep the GC from sneaking in
            if (ref != null) {
                ret = ref.get();
            }
            if ((ref == null) || (ret == null)) {
                ret = load(path);
                fileCache.put(path, new WeakReference<ROOTFile>(ret));
            }
            return ret;
        } finally {
            incrementalCleanup();
            lock.unlock();
        }
    }

    /**
     * Return the number of open ROOTFiles stored in this cache.
     * @return The number of currently open ROOTFiles
     */
    public long getOpenCount() {
        try {
            lock.lock();
            return fileCache.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the number of files in the TimedClose state
     * @return The number of files in TimedClose
     */
    public long getTimedCloseCount() {
        try {
            lock.lock();
            return timedCache.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the number of phantom references scored
     * @return The number of phantom references
     */
    public long getPhantomReferenceCount() {
        try {
            lock.lock();
            return phantomMap.size();
        } finally {
            lock.unlock();
        }
    }

    private ROOTFile load(String path) throws IOException {
        FileInterface fileInterface = timedCache.getIfPresent(path);
        if (fileInterface == null) {
            fileInterface = interfaceMap.get(path);
        }
        if (fileInterface != null) {
            /* File is in TimedClose state -- need to
             * revive it to Open
             */
            timedCache.removeIfExists(path);
        } else {
            // Can't find a reference to the fileInterface, make a new one
            fileInterface = IOFactory.openForRead(path);
            interfaceMap.put(path, fileInterface);
            logger.trace(String.format("Opening underlying %s to %s", path, fileInterface));
        }
        ROOTFile ret = ROOTFile.getInputFile(path, fileInterface);
        ROOTFileFinalizer ref = new ROOTFileFinalizer(ret, phantomQueue, ret.getFileInterface());
        phantomMap.put(path, ref);
        assert timedCache.getIfPresent(path) == null;
        return ret;
    }



    /**
     * Helper class to add additional info to PhantomReference
     *
     * <p>Since PhantomReferences don't actually hold a reference to their
     * referant (for obvious reasons), we need to have our own reference that
     * holds on to the underlying file handle, so when it's time to reap it, we
     * have a reference we can call close on
     */
    private static class ROOTFileFinalizer extends PhantomReference<ROOTFile> {
        FileInterface file;
        String path;

        public ROOTFileFinalizer(ROOTFile referent, ReferenceQueue<ROOTFile> queue, FileInterface file) {
            super(referent, queue);
            this.file = file;
            this.path = referent.getPath();
        }

        public FileInterface getFile() {
            return file;
        }

        public String getPath() {
            return path;
        }
    }

    /**
     * Helper to remove FileInterface from the timedCache.
     *
     * <p>When an element is removed from the timedCache, Guava will call back to
     * this helper to let us do cleanup tasks. Normally, this would mean we
     * should close the file and remove the reference from the phantomMap.
     * However, when we're reviving a file in TimedClose, we need to have the
     * object removed without doing the cleanup tasks.
     */
    private static class TimedCache {
        private static final long SEC_TO_NSEC = 1000000000;
        private static final long TIMEOUT = 60 * SEC_TO_NSEC;
        private HashMap<String, FileInterface> fiMap = new HashMap<String, FileInterface>();
        private HashMap<String, Long> timeMap = new HashMap<String, Long>();
        private HashMap<String, ROOTFileFinalizer> phantomMap;
        private Ticker ticker;

        public TimedCache(HashMap<String, ROOTFileFinalizer> phantomMap, Ticker ticker) {
            this.phantomMap = phantomMap;
            this.ticker = ticker;
        }

        public void removeAndCloseIfExists(String path) throws IOException {
            if (fiMap.containsKey(path)) {
                fiMap.get(path).close();
            }
            removeIfExists(path);
        }

        public void removeIfExists(String path) {
            logger.trace(String.format("Removing %s from %s", path, this));
            fiMap.remove(path);
            timeMap.remove(path);
            phantomMap.remove(path);
        }

        public void insert(String path, FileInterface val) {
            logger.trace(String.format("Inserting %s into %s", path, this));
            for (String path2: fiMap.keySet()) {
                logger.trace("contains: " + path2);
            }
            assert fiMap.containsKey(path) == false : "fi already exists " + path;
            assert timeMap.containsKey(path) == false : "timemap already exists " + path;
            fiMap.put(path,  val);
            timeMap.put(path, ticker.read());
        }

        public LinkedList<String> cleanUp() {
            LinkedList<String> toClean = new LinkedList<String>();
            long now = ticker.read();
            for (String path: timeMap.keySet()) {
                long then = timeMap.get(path);
                if ((now - then) > TIMEOUT) {
                    logger.trace(String.format("Timing out (%s > %s) %s ", (now - then) / SEC_TO_NSEC, TIMEOUT / SEC_TO_NSEC, path));
                    toClean.add(path);
                }
            }
            for (String path: toClean) {
                try {
                    removeAndCloseIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return toClean;
        }

        public int size() {
            assert fiMap.size() == timeMap.size();
            return fiMap.size();
        }

        public FileInterface getIfPresent(String path) {
            assert fiMap.size() == timeMap.size();
            assert fiMap.containsKey(path) == timeMap.containsKey(path);
            return fiMap.get(path);
        }
    }
}
