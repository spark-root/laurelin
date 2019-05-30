package edu.vanderbilt.accre.laurelin;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;

public class Cache {
    WeakHashMap<TFile, WeakHashMap<Integer, WeakReference<RawArray>>> cache;

    public Cache() {
        cache = new WeakHashMap<TFile, WeakHashMap<Integer, WeakReference<RawArray>>>();
    }

    public RawArray get(TFile backingFile, int last) {
        RawArray ret = null;
        WeakHashMap<Integer, WeakReference<RawArray>> fileMap = cache.get(backingFile);
        if (fileMap == null) {
            return null;
        }
        WeakReference<RawArray> ref = fileMap.get(last);
        if (ref == null) {
            return null;
        }
        return ref.get();
    }

    public RawArray put(TFile backingFile, int last, RawArray data) {
        WeakHashMap<Integer, WeakReference<RawArray>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, new WeakHashMap<Integer, WeakReference<RawArray>>());
            }
        }
        fileMap.put(last, new WeakReference<RawArray>(data));
        return data;
    }

}
