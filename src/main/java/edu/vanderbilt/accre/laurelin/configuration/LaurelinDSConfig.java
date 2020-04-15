package edu.vanderbilt.accre.laurelin.configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wraps the Spark-provided DataSourceOptions class to provide some level of
 * "Configuration-safety". The intention is that we declare (in ConfigListing)
 * all known Laurelin config options, their types, default values, valid ranges,
 * etc... Then, in LaurelinDSConfig, we enforce that A) the user-provided config
 * correctly declares variables properly and B) the Laurelin code accesses these
 * values properly
 */
public class LaurelinDSConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, String> map;
    private ConfigListing listing;

    public LaurelinDSConfig(Map<String, String> map, ConfigListing listing) {
        this.listing = listing;
        this.map = new HashMap<String, String>(map);
    }

    /**
     * Wrap the provided string map and return a new LaurenlinDSConfig
     * @param sparkOpts value to wrap
     */
    public static LaurelinDSConfig wrap(Map<String,String> map) {
        return new LaurelinDSConfig(map, ConfigListing.getDefaultListing());
    }

    /**
     * Returns paths associated with this config
     *
     * @return List of paths passed in via user
     */
    public List<String> paths() {
        return getPaths();
    }


    private List<String> getPaths() {
        List<String> ret = new LinkedList<String>();
        if (this.map.containsKey("paths")) {
            ObjectMapper objectMapper = new ObjectMapper();
            String pathsStr = this.map.getOrDefault("paths", "");
            String[] parsedPathsList;
            try {
                parsedPathsList = objectMapper.readValue(pathsStr, String[].class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Could not parse paths", e);
            } catch (IOException e) {
                throw new RuntimeException("Got IO exception parsing paths", e);
            }
            for (String p: parsedPathsList) {
                ret.add(p);
            }
        }

        if (this.map.containsKey("path")) {
            ret.add(this.map.get("path"));
        }
        Collections.sort(ret);
        return ret;
    }

    public Map<String, String> getMap() {
        return map;
    }

    /**
     * Returns the string value associated with the provided key. Throws an
     * unchecked exception on failure
     * @param key Configuration option we're interested in
     * @return The value associated with this key
     */
    public String getString(String key) {
        return listing.resolveValue(key, map);
    }

    /**
     * Returns the integer value associated with the provided key. Throws an
     * unchecked exception on failure
     * @param key Configuration option we're interested in
     * @return The value associated with this key
     */
    public int getInt(String key) {
        try {
            return Integer.parseInt(getString(key));
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
            return Long.parseLong(getString(key));
        } catch (ClassCastException e) {
            throw new RuntimeException("Could not cast " + key + " to a long");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(listing, map);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LaurelinDSConfig)) {
            return false;
        }
        LaurelinDSConfig other = (LaurelinDSConfig) obj;
        return Objects.equals(listing, other.listing) && Objects.equals(map, other.map);
    }
}
