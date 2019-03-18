package edu.vanderbilt.accre.root_proxy;

import java.util.ArrayList;

public class TBranch {
	private Proxy data;
	ArrayList<TBranch> branches;
	ArrayList<TLeaf> leaves; 
	public TBranch(Proxy data) {
		this.data = data;
		leaves = new ArrayList<TLeaf>();
		branches = new ArrayList<TBranch>();
		ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
		for (Proxy val: fBranches) {
			branches.add(new TBranch(val));
		}
		ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
		for (Proxy val: fLeaves) {
			leaves.add(new TLeaf(val));
		}
	}
	
	public String getName() {
		return (String) data.getScalar("fName").getVal();
	}
}
