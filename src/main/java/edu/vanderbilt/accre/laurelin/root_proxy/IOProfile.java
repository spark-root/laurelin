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
        public class Storage implements Serializable {
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

        }

        Storage storage;
        IOProfile parent;

        private Event() {
            storage = new Storage();
        }

        protected static Event startOp(IOProfile ioProfile, long offset, int len, int count, int fid, int eid) {
            Event event = new Event();
            event.parent = ioProfile;
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

    int fid;
    int eid;
    Function<Event, Integer> callback;

    public IOProfile(int eid, Function<Event, Integer> callback) {
        this.eid = eid;
        this.callback = callback;
    }

    public void setFid(int fid) {
        this.fid = fid;
    }

    public void incrementFid() {
        this.fid = globalFid.addAndGet(1);
    }

    public Event startOp(long offset, int len) {
        return Event.startOp(this, offset, len, eventCount.addAndGet(1), fid, eid);
    }

    public Function<Event, Integer> getCallback() {
        return callback;
    }

    static AtomicInteger eventCount = new AtomicInteger();
    static AtomicInteger globalFid = new AtomicInteger();

}
