package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.ArrayList;
import java.util.List;

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
			TBranch branch = new TBranch(val);
			// Drop branches with neither subbranches nor leaves
			if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
				branches.add(branch);
			}
		}
		ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
		for (Proxy val: fLeaves) {
			TLeaf leaf = new TLeaf(val);
			if (leaf.typeUnhandled()) {
				continue;
			}
			leaves.add(leaf);
		}
	}
	
	public String getTitle() {
		return (String) data.getScalar("fTitle").getVal();
	}
	
	public String getName() {
		return (String) data.getScalar("fName").getVal();
	}
	
	public List<TBranch> getBranches() {
		return branches;
	}
	
	public List<TLeaf> getLeaves() {
		return leaves;
	}
}
