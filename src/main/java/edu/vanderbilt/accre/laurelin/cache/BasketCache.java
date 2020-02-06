package edu.vanderbilt.accre.laurelin.cache;

import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.ROOTFile;

public class BasketCache {
    private static BasketCache singleton = new BasketCache();

    public static synchronized BasketCache getCache() {
        return singleton;
    }

    private BasketCache() {
        cache = Collections.synchronizedMap(new WeakHashMap<ROOTFile, Map<Long, SoftReference<RawArray>>>());
    }

    Map<ROOTFile, Map<Long, SoftReference<RawArray>>> cache;
    int totalCount = 0;
    int hitCount = 0;
    int missCount = 0;
    long putBytes = 0;
    long getBytes = 0;

    public RawArray get(ROOTFile backingFile, long offset) {
        totalCount += 1;
        Map<Long, SoftReference<RawArray>> fileMap = cache.get(backingFile);
        if (fileMap == null) {
            missCount += 1;
            return null;
        }
        synchronized (fileMap) {
            SoftReference<RawArray> ref = fileMap.get(offset);
            if (ref == null) {
                missCount += 1;
                return null;
            }
            hitCount += 1;
            RawArray ret = ref.get();
            if (ret == null) {
                missCount += 1;
                return null;
            }
            getBytes += ret.length();
            return ret;
        }
    }

    public RawArray put(ROOTFile backingFile, long offset, RawArray data) {
        Map<Long, SoftReference<RawArray>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, Collections.synchronizedMap(new HashMap<Long, SoftReference<RawArray>>()));
            }
        }
        synchronized (fileMap) {
            fileMap.put(offset, new SoftReference<RawArray>(data));
            putBytes += data.length();
            return data;
        }
    }
}
