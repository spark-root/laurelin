package edu.vanderbilt.accre.root_proxy;

public class ProxyElement<T> extends Proxy {
	/*
	 * One element within a proxy of a POD-like type
	 */
	private T val;
	public ProxyElement() { }
	public ProxyElement(T newval) {
		val = newval;
	}
	public T getVal() {
		return val;
	}
	public void setVal(T newval) {
		val = newval;
	}
    @Override
	public void dump(int depth) {
		dumpData(depth);
	}
    @Override
	public void dumpData(int depth) {
		String indent = "";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }
		//System.out.println(indent + val);
	}
}
