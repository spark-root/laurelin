package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.ArrayList;

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
	private ArrayList<TBasket> baskets;
		
	public TTree(Proxy data) {
		this.data = data;
		branches = new ArrayList<TBranch>();
		leaves = new ArrayList<TLeaf>();
		baskets = new ArrayList<TBasket>();
		ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: fBranches) {
			// Drop branches with neither subbranches nor leaves
			TBranch branch = new TBranch(val);
			if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
				branches.add(branch);
			} else {
				System.out.println("Ignoring unparsable branch " + branch.getName());
			}
		}
		
		// TODO: fill leaves/baskets - the trick is to make sure that the right
		// references are passed around instead of possibly duplicating objects	
//		ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
//		for (Proxy val: fLeaves) {
//			TLeaf leaf = new TLeaf(val);
//			if (leaf.typeUnhandled()) {
//				continue;
//			}
//			leaves.add(leaf);
//		}		
//		ProxyArray fBaskets = (ProxyArray) data.getProxy("fBaskets");
//		for (Proxy val: fBaskets) {
//			baskets.add(new TBasket(val));
//		}
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
	
	public int getEntries() {
		return (int) data.getScalar("fEntries").getVal();
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
