package edu.vanderbilt.accre.root_proxy;

import java.io.IOException;
import java.util.HashMap;
public class Proxy {
	/*
	 * Stores raw information for one ROOT object
	 */
	protected String className;
	protected String proxyType = "proxy";
	HashMap<String, Proxy> data;
	public <T> void putScalar(String key, T val) {
		data.put(key, new ProxyElement<T>(val));
	}
	public <T> void putProxy(String key, T val) {
		data.put(key, (Proxy) val);
	}
	
	@SuppressWarnings("unchecked")
	public <T> ProxyElement<T> getScalar(String key) {
		return (ProxyElement<T>) data.get(key);
	}
	
	public Proxy getProxy(String key) {
		return data.get(key);
	}

	public Proxy() {
		data = new HashMap<String, Proxy>();
		className = "UNKNOWN";
	}
	
	public void dump() {
		dump(0);
	}
	
	public void dump(int depth) {
		dumpData(depth);
	}

	public void dumpData(int depth) {
		String indent = "";
		for (int i = 0; i <= depth; i += 1) { indent += "  "; }
		for (String key : data.keySet()) {
			Proxy p = data.get(key);
			if (p == null) {
				System.out.println(indent + key + " NULL");
			} else {
				System.out.println(indent + key + " " + p.proxyType + "(" + p.className + "): ");
				data.get(key).dump(depth + 1);
			}
		}
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
