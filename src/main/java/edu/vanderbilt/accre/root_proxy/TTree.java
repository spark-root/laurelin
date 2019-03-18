package edu.vanderbilt.accre.root_proxy;

import java.util.ArrayList;

public class TTree {
	private Proxy data;
	ArrayList<TBranch> branches;
	ArrayList<TLeaf> leaves;
	ArrayList<TBasket> baskets;
	
	public TTree(Proxy data) {
		this.data = data;
		branches = new ArrayList<TBranch>();
		leaves = new ArrayList<TLeaf>();
		baskets = new ArrayList<TBasket>();
		ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: fBranches) {
			branches.add(new TBranch(val));
		}
		// TODO: fill leaves/baskets - the trick is to make sure that the right
		// references are passed around instead of possibly duplicating objects
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
	
	public void iterate(String[] branchNames) {
		for (String val: data.data.keySet()) {
			System.out.println(" - " + val);
		}
		
		ProxyArray branches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: branches) {
			String fName = (String) val.getScalar("fName").getVal();
			System.out.println(" - " + fName + " - " + val.hashCode());
			for (String val2: val.data.keySet()) {
				if (!val2.startsWith("fBasket")) {
					continue;
				}
				Object val3 = ((ProxyElement) val.data.get(val2));
				System.out.println("   - " + val2 + " - " + ((String) val3));
			}
		}
		System.out.println("-----");
		ProxyArray leaves = (ProxyArray) data.getProxy("fLeaves");
		for (Proxy val: leaves) {
			System.out.println(" - " + (String) val.getScalar("fName").getVal() + " - " + val.hashCode());
			for (String val2: val.data.keySet()) {
				System.out.println("   - " + val2 + " - " + val.data.get(val2));
			}
		}
	}
}
