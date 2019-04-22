package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
}
