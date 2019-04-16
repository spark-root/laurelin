package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.HashMap;
public class Proxy {
	/*
	 * Stores raw information for one ROOT object
	 */
	private Streamer streamerInfo;
	protected String className;
	protected String proxyType = "proxy";
	HashMap<String, Proxy> data;
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
	
	public String dump() {
		return dump(0);
	}
	
	public String dump(int depth) {
		return dumpData(depth);
	}

	public String dumpData(int depth) {
		String indent = "";
		String ret = "";
		ret += "dump";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }
		for (String key : data.keySet()) {
			Proxy p = data.get(key);
			if (p == null) {
				ret += indent + " - " + key;
			} else {
				ret += data.get(key).dump(depth + 1);
			}
		}
		return ret;
	}
	
	public void setClass(String className) {
		this.className = className;
	}
	private ClassDeserializer cd;
	public ClassDeserializer getDeserializer() { return cd; }
	public void setDeserializer(ClassDeserializer newcd) { cd = newcd; }
	
	/**
	 * @deprecated Use {@link Streamer#readObjAny(Cursor,HashMap<Long, Proxy>)} instead
	 */
//	static Proxy readObjAny(Cursor cursor, HashMap<Long,Proxy> classMap) throws IOException {
//		return Streamer.readObjAny(cursor, classMap);
//	}
}
