package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.HashMap;

public class Proxy {
    /*
     * Stores raw information for one ROOT object
     */
    private Streamer streamerInfo;
    protected String className;
    protected String proxyType = "proxy";
    public String createPlace = "";
    HashMap<String, Proxy> data;
    private ClassDeserializer cd;

    public <T> void putScalar(String key, T val) {
        data.put(key, new ProxyElement<T>(val));
    }

    public <T> void putProxy(String key, T val) {
        data.put(key, (Proxy) val);
    }

    public String getClassName() {
        return className;
    }

    @SuppressWarnings("unchecked")
    public <T> ProxyElement<T> getScalar(String key) {
        return (ProxyElement<T>) data.get(key);
    }

    public Proxy getProxy(String key) {
        return data.get(key);
    }

    public int getDataSize() {
        return data.size();
    }

    public Proxy() {
        data = new HashMap<String, Proxy>();
        className = "UNKNOWN";
    }

    public void setClass(String className) {
        this.className = className;
    }


    public ClassDeserializer getDeserializer() {
        return cd;
    }

    public void setDeserializer(ClassDeserializer newcd) {
        cd = newcd;
    }

    public void dump() {
        System.out.println(dumpStr());
    }

    public String dumpStr() {
        return dumpStr(0);
    }

    public String dumpStr(int depth) {
        String indent = "";
        for (int i = 0; i < depth; i += 1) {
            indent += "  ";
        }
        String ret = indent;
        for (String key: data.keySet()) {
            ret += indent + key + " - " + data.get(key).dumpStr(depth + 1) + "\n";
        }
        return ret;
    }

}
