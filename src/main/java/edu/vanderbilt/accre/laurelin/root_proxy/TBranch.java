package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TBranch {
	protected Proxy data;
	protected ArrayList<TBranch> branches;
	private ArrayList<TLeaf> leaves;
	private ArrayList<TBasket> baskets;

	protected boolean isBranch;
	protected TBranch parent;
	protected TTree tree;
	
	public TBranch(Proxy data, TTree tree, TBranch parent) {
		this.data = data;
		this.parent = parent;
		this.tree = tree;

		branches = new ArrayList<TBranch>();
		if (getClass().equals(TBranch.class)) {
			baskets = new ArrayList<TBasket>();
			leaves = new ArrayList<TLeaf>();
			isBranch = true;
			ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
			for (Proxy val: fBranches) {
				TBranch branch = new TBranch(val, tree, this);
				// Drop branches with neither subbranches nor leaves
				if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
					branches.add(branch);
				}
			}
			ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
			for (Proxy val: fLeaves) {
				TLeaf leaf = new TLeaf(val, tree, this);
				if (leaf.typeUnhandled()) {
					continue;
				}
				leaves.add(leaf);
			}
			
			/*
			 * Instead of being in the ObjArray of fBaskets, ROOT stores the baskets in separate
			 * toplevel entries in the file
			 *     Int_t      *fBasketBytes;      ///<[fMaxBaskets] Length of baskets on file
    		 *     Long64_t   *fBasketEntry;      ///<[fMaxBaskets] Table of first entry in each basket
    		 *     Long64_t   *fBasketSeek;       ///<[fMaxBaskets] Addresses of baskets on file
			 */
			int fMaxBaskets = (int) data.getScalar("fMaxBaskets").getVal();
			int[] fBasketBytes = (int[]) data.getScalar("fBasketBytes").getVal();
			long[] fBasketEntry = (long[]) data.getScalar("fBasketEntry").getVal();
			long[] fBasketSeek = (long[]) data.getScalar("fBasketSeek").getVal();
			baskets = new ArrayList<TBasket>();
			TFile backing = tree.getBackingFile();
			for (int i = 0; i < fMaxBaskets; i += 1) {
				Cursor c;
				if (fBasketSeek[i] == 0) {
					// An empty basket?
					continue;
				}
				try {
					c = backing.getCursorAt(fBasketSeek[i]);
					TBasket b = TBasket.getFromFile(c, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i]);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		} else {
			isBranch = false;
		}
	}
	
	public String getTitle() {
		return (String) data.getScalar("fTitle").getVal();
	}
	
	public String getName() {
		return (String) data.getScalar("fName").getVal();
	}
	
	public String getClassName() {
		return data.getClassName();
	}
	
	public List<TBranch> getBranches() {
		return branches;
	}
	
	public List<TLeaf> getLeaves() {
		return leaves;
	}
	
	public boolean typeUnhandled() { return false; }

	public SimpleType getSimpleType() { return null; }
}
