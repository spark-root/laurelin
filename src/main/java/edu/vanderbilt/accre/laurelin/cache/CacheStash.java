package edu.vanderbilt.accre.laurelin.cache;

public class CacheStash {
    private static Cache cache;

    public static Cache getCache() {
        // Racy
        if (cache == null) {
            cache = new Cache();
        }
        return cache;
    }
}