package edu.vanderbilt.accre.root_proxy;

public class TLeaf {
	private Proxy data;
	public TLeaf(Proxy data) {
		this.data = data;
	}
	
	public String getName() {
		return (String) data.getScalar("fName").getVal();
	}
}
