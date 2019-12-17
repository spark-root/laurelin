package edu.vanderbilt.accre.laurelin.cache;

import java.io.Serializable;

/**
 * This isn't a cache, this is an object that makes a cache. This object is made
 * on the Spark driver and transmitted to the executors, who in turn make the
 * actual caches on the executors.
 *<p>
 * This gets around the need to be able to serialize and transmit Partition
 * objects
 */
public class CacheFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    public Cache getCache() {
        return CacheStash.getCache();
    }
}
