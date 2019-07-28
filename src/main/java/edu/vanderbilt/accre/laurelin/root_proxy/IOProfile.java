package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage.TypeEnum;

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


            public enum TypeEnum {
                LOWER,
                UPPER
            }

            /**
             * There are two types of IO we care about - "Upper" IO, is IO from
             * the application before any caching/prefetching/etc occurs.
             * "Lower" IO is a request that reaches an actual filesystem call.
             */
            public TypeEnum type;

        }

        Storage storage;
        FileProfiler parent;

        private Event() {
            storage = new Storage();
        }

        protected static Event startOp(FileProfiler fileProfiler, long offset, int len, int count, int fid, int eid, TypeEnum type) {
            Function<Event, Integer> cb = fileProfiler.getCallback();
            if (cb != null) {
                Event event = new Event();
                event.parent = fileProfiler;
                event.storage.offset = offset;
                event.storage.len = len;
                event.storage.startTime = System.nanoTime();
                event.storage.count = count;
                event.storage.eid = eid;
                event.storage.fid = fid;
                event.storage.type = type;
                return event;
            } else {
                Event event = new Event();
                event.parent = fileProfiler;
                return event;
            }
        }

        @Override
        public void close() throws Exception {
            Function<Event, Integer> cb = parent.getCallback();
            if (cb != null) {
                this.storage.endTime = System.nanoTime();
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

        private Event getFileLoadEvent(int eid, int fid, String path, TypeEnum type) {
            Event.Storage init = new IOProfile.Event.Storage();
            init.fileName = path;
            init.fid = fid;
            init.eid = eid;
            init.count = eventID.addAndGet(1);
            init.offset = 0;
            init.len = 0;
            init.type = TypeEnum.UPPER;
            Event ev = new IOProfile.Event();
            ev.storage = init;
            return ev;
        }

        protected FileProfiler(int eid, int fid, String path, Function<Event, Integer> callback) {
            this.fid = fid;
            this.eid = eid;
            this.path = path;
            this.callback = callback;

            /*
             * Send in a null event with the filename so we can later parse if
             * we need to backtrace what filename belongs to a given fid
             */
            if (callback != null) {
                callback.apply(getFileLoadEvent(eid, fid, path, TypeEnum.UPPER));
                callback.apply(getFileLoadEvent(eid, fid, path, TypeEnum.LOWER));
            }
        }

        public Event startLowerOp(long offset, int len) {
            return Event.startOp(this, offset, len, eventID.addAndGet(1), fid, eid, TypeEnum.LOWER);
        }

        public Event startUpperOp(long offset, int len) {
            return Event.startOp(this, offset, len, eventID.addAndGet(1), fid, eid, TypeEnum.UPPER);
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