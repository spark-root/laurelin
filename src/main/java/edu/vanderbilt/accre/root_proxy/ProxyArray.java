package edu.vanderbilt.accre.root_proxy;

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
	public void dump(int depth) {
		dumpData(depth);
		dumpArray(depth);
	}
	public void dumpArray(int depth) {
		String indent = " ";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }

		for (Proxy p: val) {
			//System.out.println(indent + " " )
			if (p == null) {
				System.out.println(indent + " - NULL");
				continue;
			}
			System.out.println(indent + "- (" + p.className + "): ");
			p.dump(depth + 1);
		}
	}
	
}
