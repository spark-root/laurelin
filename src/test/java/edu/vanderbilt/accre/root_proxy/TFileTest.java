package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TFileTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testOpen() throws IOException {
		TFile testfile = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
		System.out.println("****** load tree *****");
		Proxy events = testfile.get("tree");
		//events.dump();
		System.out.println("Lloaded file " + events);
		TTree tree = new TTree(events);
		tree.iterate(null);
	}

}
