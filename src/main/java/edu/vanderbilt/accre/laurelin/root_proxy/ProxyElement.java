package edu.vanderbilt.accre.laurelin.root_proxy;

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
	public String dump(int depth) {
		return dumpData(depth);
	}
    @Override
	public String dumpData(int depth) {
    	String ret = "";
		String indent = "";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }
		return ret;
	}
}
