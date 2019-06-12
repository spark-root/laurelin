package edu.vanderbilt.accre.root_proxy;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.Proxy;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;

public class TFileTest {

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
    }

    @Test(expected = NoSuchElementException.class)
    public void testFail() throws IOException {
        TFile testfile = TFile.getFromFile("testdata/nano_tree.root");
        Proxy events = testfile.getProxy("tree");
        TTree tree = new TTree(events, testfile);
        tree.iterate(new String[] {"N"});
    }
}
