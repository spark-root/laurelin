package edu.vanderbilt.accre.laurelin.root_proxy.serialization;

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

    public int size() {
        return val.size();
    }

    public Proxy at(int idx) {
        return val.get(idx);
    }

}
