package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/*
 * Toolkit to profile IO, specifically storing operations performed and their
 * timings
 */
public class IOProfile {
    public static class Event implements AutoCloseable {
        public static class Storage implements Serializable {
            /**
             * This is the class that actually gets shipped from the executor
             * back to the driver. The Event wrapper holds other info
             *
             */
            private static final long serialVersionUID = 1L;

            /**
             * Unique file id, used to separate out things happening to
             * different files
             */
            public int fid;

            /**
             * Executor ID
             */
            public int eid;

            /**
             * Unique ID for a given executor processing a given file
             */
            public int count;
            public long offset;
            public int len;
            public long startTime;
            public long endTime;
            /**
             * Only set on open to keep from transmitting it over and over
             */
            public String fileName;

        }

        Storage storage;
        FileProfiler parent;

        private Event() {
            storage = new Storage();
        }

        protected static Event startOp(FileProfiler fileProfiler, long offset, int len, int count, int fid, int eid) {
            Event event = new Event();
            event.parent = fileProfiler;
            event.storage.offset = offset;
            event.storage.len = len;
            event.storage.startTime = System.nanoTime();
            event.storage.count = count;
            event.storage.eid = eid;
            event.storage.fid = fid;
            return event;
        }

        @Override
        public void close() throws Exception {
            this.storage.endTime = System.nanoTime();
            Function<Event, Integer> cb = parent.getCallback();
            if (cb != null) {
                cb.apply(this);
            }
        }

        public Storage getStorage() {
            return storage;
        }
    }

    public static class FileProfiler {
        int fid;
        int eid;
        String path;
        Function<Event, Integer> callback;
        static AtomicInteger eventID = new AtomicInteger();

        protected FileProfiler(int fid, int eid, String path, Function<Event, Integer> callback) {
            this.fid = fid;
            this.eid = eid;
            this.path = path;
            this.callback = callback;

            /*
             * Send in a null event with the filename so we can later parse if
             * we need to backtrace what filename belongs to a given fid
             */
            if (callback != null) {
                Event.Storage init = new IOProfile.Event.Storage();
                init.fileName = path;
                init.fid = fid;
                init.eid = eid;
                init.count = eventID.addAndGet(1);
                init.offset = 0;
                init.len = 0;
                Event ev = new IOProfile.Event();
                ev.storage = init;
                callback.apply(ev);
            }
        }

        public Event startOp(long offset, int len) {
            return Event.startOp(this, offset, len, eventID.addAndGet(1), fid, eid);
        }

        public Function<Event, Integer> getCallback() {
            return callback;
        }

    }

    static AtomicInteger globalFid = new AtomicInteger();
    int eid;
    Function<Event, Integer> callback;

    private IOProfile() {
        this.eid = -1;
    }

    private IOProfile(int eid, Function<Event, Integer> callback) {
        this.eid = eid;
        this.callback = callback;
    }

    public void setCB(Function<Event, Integer> cb) {
        this.callback = cb;
    }

    public FileProfiler beginProfile(String path) {
        return new FileProfiler(eid, globalFid.getAndAdd(1), path, callback);
    }

    private static IOProfile instance;

    public static synchronized IOProfile getInstance() {
        return getInstance(-1, null);
    }

    public static synchronized IOProfile getInstance(int eid, Function<Event, Integer> callback) {
        if (instance == null) {
            instance = new IOProfile(eid, callback);
        }
        if (callback != null) {
            instance.callback = callback;
        }
        return instance;
    }
}