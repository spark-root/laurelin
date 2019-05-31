package edu.vanderbilt.accre.laurelin;

import java.lang.ref.SoftReference;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;

public class Cache {
    WeakHashMap<ROOTFile, WeakHashMap<Integer, SoftReference<RawArray>>> cache;

    public Cache() {
        cache = new WeakHashMap<ROOTFile, WeakHashMap<Integer, SoftReference<RawArray>>>();
    }

    public RawArray get(ROOTFile backingFile, int last) {
        WeakHashMap<Integer, SoftReference<RawArray>> fileMap = cache.get(backingFile);
        if (fileMap == null) {
            return null;
        }
        SoftReference<RawArray> ref = fileMap.get(last);
        if (ref == null) {
            return null;
        }
        return ref.get();
    }

    public RawArray put(ROOTFile backingFile, int last, RawArray data) {
        WeakHashMap<Integer, SoftReference<RawArray>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, new WeakHashMap<Integer, SoftReference<RawArray>>());
            }
        }
        fileMap.put(last, new SoftReference<RawArray>(data));
        return data;
    }

}
