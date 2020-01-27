package edu.vanderbilt.accre.laurelin.configuration;

import org.apache.spark.sql.sources.v2.DataSourceOptions;

/**
 * Wraps the Spark-provided DataSourceOptions class to provide some level of
 * "Configuration-safety". The intention is that we declare (in ConfigListing)
 * all known Laurelin config options, their types, default values, valid ranges,
 * etc... Then, in LaurelinDSConfig, we enforce that A) the user-provided config
 * correctly declares variables properly and B) the Laurelin code accesses these
 * values properly
 */
public class LaurelinDSConfig  {
    private DataSourceOptions sparkOpts;
    private ConfigListing listing;

    /**
     * Necessary public constructor to wrap
     * @param sparkOpts user config
     * @param listing ConfigListing object describing valid options
     */
    public LaurelinDSConfig(DataSourceOptions sparkOpts, ConfigListing listing) {
        this.sparkOpts = sparkOpts;
        this.listing = listing;
        listing.validateConfigMap(sparkOpts.asMap());
    }

    /**
     * Wrap the provided DataSourceOptions and return a new LaurenlinDSConfig
     * @param sparkOpts value to wrap
     */
    public static LaurelinDSConfig wrap(DataSourceOptions sparkOpts) {
        return new LaurelinDSConfig(sparkOpts, ConfigListing.getDefaultListing());
    }

    /**
     * Returns paths associated with this config
     *
     * @return List of paths passed in via user
     */
    public String[] paths() {
        return sparkOpts.paths();
    }

    private String getGeneric(String key) {
        return listing.resolveValue(key, sparkOpts.asMap());
    }

    /**
     * Returns the string value associated with the provided key. Throws an
     * unchecked exception on failure
     * @param key Configuration option we're interested in
     * @return The value associated with this key
     */
    public String getString(String key) {
        return getGeneric(key);
    }

    /**
     * Returns the integer value associated with the provided key. Throws an
     * unchecked exception on failure
     * @param key Configuration option we're interested in
     * @return The value associated with this key
     */
    public int getInt(String key) {
        try {
            return Integer.parseInt(getGeneric(key));
        } catch (ClassCastException e) {
            throw new RuntimeException("Could not cast " + key + " to an int");
        }
    }

    /**
     * Returns the long value associated with the provided key. Throws an
     * unchecked exception on failure
     * @param key Configuration option we're interested in
     * @return The value associated with this key
     */
    public long getLong(String key) {
        try {
            return Long.parseLong(getGeneric(key));
        } catch (ClassCastException e) {
            throw new RuntimeException("Could not cast " + key + " to a long");
        }
    }
}
