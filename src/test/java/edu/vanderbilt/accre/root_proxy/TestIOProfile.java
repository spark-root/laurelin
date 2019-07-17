package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.function.Function;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;

public class TestIOProfile {

    @Test
    public void testNullCallback() throws Exception {
        IOProfile profiler = new IOProfile(1, null);
        try (Event ev = profiler.startOp(0, 1)) {

        }
    }

    @Test
    public void testRealCallback() throws Exception {
        LinkedList<Storage> val = new LinkedList<Storage>();
        Function<Event, Integer> cb = e -> {
            val.add(e.getStorage());
            return 0;
        };
        IOProfile profiler = new IOProfile(1, cb);
        try (Event ev = profiler.startOp(10, 20)) {
            assertEquals(0, val.size());
        }
        assertEquals(1, val.size());
        Storage stor = val.get(0);
        assertEquals(10, stor.offset);
        assertEquals(20, stor.len);
    }

}
