package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.function.Function;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.FileProfiler;

public class TestIOProfile {

    @Test
    public void testNullCallback() throws Exception {
        IOProfile profiler = IOProfile.getInstance(1, null);
        FileProfiler f = profiler.beginProfile("test-path");
        try (Event ev = f.startOp(0, 1)) {

        }
    }

    @Test
    public void testRealCallback() throws Exception {
        LinkedList<Storage> val = new LinkedList<Storage>();
        Function<Event, Integer> cb = e -> {
            val.add(e.getStorage());
            return 0;
        };
        IOProfile profiler = IOProfile.getInstance(1,cb);
        FileProfiler f = profiler.beginProfile("test-path");
        try (Event ev = f.startOp(10, 20)) {
            assertEquals(0, val.size());
        }
        assertEquals(1, val.size());
        Storage stor = val.get(0);
        assertEquals(10, stor.offset);
        assertEquals(20, stor.len);
    }

}
