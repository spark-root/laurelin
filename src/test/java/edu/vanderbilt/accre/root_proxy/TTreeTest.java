package edu.vanderbilt.accre.root_proxy;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;

public class TTreeTest {

	private TTree getTestTree() throws IOException {
		TFile currFile = TFile.getFromFile("testdata/uproot-small-flat-tree.root");
		return new TTree(currFile.getProxy("tree"), currFile);
	}
	
	@Test
	public void testEntryCount() throws IOException {
		TTree currTree = getTestTree();
		assertEquals(100, currTree.getEntries());
	}
	
	@Test
	public void testGetBranch() throws IOException {
		TTree currTree = getTestTree();
		List<TBranch> branches = currTree.getBranches();
		assertEquals(19, branches.size());
	}

}
