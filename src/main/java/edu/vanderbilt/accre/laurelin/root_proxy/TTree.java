package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TTree {
	private class Iterator {
		private TTree target;
		private String[] branchNames;
		public Iterator(TTree target, String[] branchNames) {
			this.target = target;
			this.branchNames = branchNames;
		}

	}

	private Proxy data;
	private ArrayList<TBranch> branches;
	private ArrayList<TLeaf> leaves;
	private TFile file;
	private static final Logger logger = LogManager.getLogger();

	public TTree(Proxy data, TFile file) {
		this.data = data;
		this.file = file;
		branches = new ArrayList<TBranch>();
		leaves = new ArrayList<TLeaf>();
		ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: fBranches) {
			// Drop branches with neither subbranches nor leaves
			TBranch branch = new TBranch(val, this, null);
			if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
				branches.add(branch);
			} else {
				// TODO: would be good to have a "one-off" log4j log
				logger.info("Ignoring unparsable/empty branch \"{}\"", branch.getName());
			}
		}
	}

	public TFile getBackingFile() {
		return file;
	}

	public ArrayList<TBranch> getBranches() {
		return branches;
	}

	public ArrayList<TLeaf> getLeaves() {
		return leaves;
	}

	public String getName() {
		return (String) data.getScalar("fName").getVal();
	}

	public long getEntries() {
		return (long) data.getScalar("fEntries").getVal();
	}

	public double[] getIndexValues() {
		return (double []) data.getScalar("fIndexValues").getVal();
	}

	public int[] getIndex() {
		return (int []) data.getScalar("fIndex").getVal();
	}

	public Iterator iterate(String[] branchNames) {
		ProxyArray branches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: branches) {
			for (String val2: val.data.keySet()) {
				if (!val2.startsWith("fBasket")) {
					continue;
				}
			}
		}
		ProxyArray leaves = (ProxyArray) data.getProxy("fLeaves");
		for (Proxy val: leaves) {

		}

		return new Iterator(this, branchNames);
	}
}
