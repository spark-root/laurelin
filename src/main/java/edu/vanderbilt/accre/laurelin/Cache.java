package edu.vanderbilt.accre.laurelin;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;

public class Cache {
    WeakHashMap<ROOTFile, HashMap<Long, SoftReference<RawArray>>> cache;
    int totalCount = 0;
    int hitCount = 0;
    int missCount = 0;

    public Cache() {
        cache = new WeakHashMap<ROOTFile, HashMap<Long, SoftReference<RawArray>>>();
    }

    public RawArray get(ROOTFile backingFile, long offset) {
        totalCount += 1;
        HashMap<Long, SoftReference<RawArray>> fileMap = cache.get(backingFile);
        if (fileMap == null) {
            missCount += 1;
            return null;
        }
        SoftReference<RawArray> ref = fileMap.get(offset);
        if (ref == null) {
            missCount += 1;
            return null;
        }
        hitCount += 1;
        return ref.get();
    }

    public RawArray put(ROOTFile backingFile, long offset, RawArray data) {
        HashMap<Long, SoftReference<RawArray>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, new HashMap<Long, SoftReference<RawArray>>());
            }
        }
        fileMap.put(offset, new SoftReference<RawArray>(data));
        return data;
    }
}