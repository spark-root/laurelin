package edu.vanderbilt.accre.laurelin.cache;

import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;

import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFile;

public class BasketCache {
	private static class CacheKey {
		public long offset;
		public long parentOffset;

		public CacheKey(long offset, long parentOffset) {
			this.offset = offset;
			this.parentOffset = parentOffset;
		}

		@Override
		public int hashCode() {
			return Objects.hash(offset, parentOffset);
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof CacheKey)) {
				return false;
			}
			CacheKey other = (CacheKey) obj;
			return offset == other.offset && parentOffset == other.parentOffset;
		}
	}
    private static BasketCache singleton = new BasketCache();

    public static synchronized BasketCache getCache() {
        return singleton;
    }

    private BasketCache() {
        cache = Collections.synchronizedMap(new WeakHashMap<ROOTFile, Map<CacheKey, SoftReference<RawArray>>>());
    }

    Map<ROOTFile, Map<CacheKey, SoftReference<RawArray>>> cache;
    int totalCount = 0;
    int hitCount = 0;
    int missCount = 0;
    long putBytes = 0;
    long getBytes = 0;

    public RawArray get(ROOTFile backingFile, long offset, long parentOffset) {
        totalCount += 1;
        Map<CacheKey, SoftReference<RawArray>> fileMap = cache.get(backingFile);
        if (fileMap == null) {
            missCount += 1;
            return null;
        }
        synchronized (fileMap) {
            SoftReference<RawArray> ref = fileMap.get(new CacheKey(offset, parentOffset));
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

    public RawArray put(ROOTFile backingFile, long offset, long parentOffset, RawArray data) {
        Map<CacheKey, SoftReference<RawArray>> fileMap = null;
        while (fileMap == null) {
            fileMap = cache.get(backingFile);
            if (fileMap == null) {
                cache.putIfAbsent(backingFile, Collections.synchronizedMap(new HashMap<CacheKey, SoftReference<RawArray>>()));
            }
        }
        synchronized (fileMap) {
            fileMap.put(new CacheKey(offset, parentOffset), new SoftReference<RawArray>(data));
            putBytes += data.length();
            return data;
        }
    }
}
