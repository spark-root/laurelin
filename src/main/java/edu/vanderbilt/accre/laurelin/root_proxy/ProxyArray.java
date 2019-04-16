package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.ArrayList;
import java.util.Iterator;

public class ProxyArray extends Proxy implements Iterable<Proxy>  {
	/*
	 * An array of proxy objects
	 */
	protected String proxyType = "proxyarray";
	ArrayList<Proxy> val;
	public ProxyArray() {
		val = new ArrayList<Proxy>();
	}
	
	void add(Proxy x) {
		val.add(x);
	}
	
    @Override
    public Iterator<Proxy> iterator() {
    	return val.iterator();
    }
    
    @Override
	public String dump(int depth) {
		return dumpData(depth) + dumpArray(depth);
	}
	public String dumpArray(int depth) {
		String indent = " ";
		String ret = "";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }

		for (Proxy p: val) {
			if (p == null) {
				continue;
			}
			ret += p.dump(depth + 1);
		}
		return ret;
	}
	
}
