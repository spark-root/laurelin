package edu.vanderbilt.accre.laurelin.root_proxy;

import static edu.vanderbilt.accre.laurelin.Helpers.getBigTestDataIfExists;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;

public class TFileTest {
    List<Storage> accum;
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() {
        accum = new LinkedList<Storage>();
        Function<Event, Integer> cb = e -> {
            accum.add(e.getStorage());
            return 0;
        };
        IOProfile.getInstance().setCB(cb);
    }

    @Test
    public void testOpen() throws IOException {
        TFile testfile = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
        Proxy events = testfile.getProxy("tree");
        TTree tree = new TTree(events, testfile);
        tree.iterate(new String[] {"N"});
    }

    @Test
    public void testOpenNanoAOD() throws IOException {
        TFile testfile = TFile.getFromFile("testdata/nano_tree.root");
        Proxy events = testfile.getProxy("Events");
        TTree tree = new TTree(events, testfile);
        for (Storage val: accum) {
            if (val.type == Storage.TypeEnum.LOWER) {
                logger.trace("loaded " + val.len + " bytes at " + val.offset);
            }
        }
        IOProfile instance = IOProfile.getInstance();
    }

    @Test
    public void testOpenUncompressed() throws IOException {
        TFile testfile = TFile.getFromFile(getBigTestDataIfExists("testdata/uproot-cases/sample-6.18.00-uncompressed.root"));
    }

    @Test(expected = NoSuchElementException.class)
    public void testFail() throws IOException {
        TFile testfile = TFile.getFromFile("testdata/nano_tree.root");
        Proxy events = testfile.getProxy("tree");
        TTree tree = new TTree(events, testfile);
        tree.iterate(new String[] {"N"});
    }
}
