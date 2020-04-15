package edu.vanderbilt.accre.laurelin.configuration;


import edu.vanderbilt.accre.laurelin.configuration.ConfigListing.ConfigBuilder;
import edu.vanderbilt.accre.laurelin.configuration.ConfigListing.ConfigOption;
import edu.vanderbilt.accre.laurelin.configuration.ConfigListing.ConfigValueType;
import edu.vanderbilt.accre.laurelin.configuration.ConfigListing.IntegerValueType;
import edu.vanderbilt.accre.laurelin.configuration.ConfigListing.StringValueType;

/**
 * The canonical list of all available Laurelin options
 */
public class SparkOptions {
    public static ConfigOption[] getConfigList() {
        return  new ConfigOption[] {
                newConfig("threadCount")
                    .type(INTEGER_TYPE)
                    .literalDefault("16")
                    .description("Number of (system-wide) threads to use for background I/O and decompression")
                    .build(),
                newConfig("tree")
                    .type(STRING_TYPE)
                    .literalDefault("Events")
                    .description("The name of the TTree to load from our file(s)")
                    .build(),
                newConfig("path")
                    .build(),
                newConfig("paths")
                    .build(),
        };
    }

    public static ConfigBuilder newConfig(String name) {
        return new ConfigBuilder(name);
    }

    private static final ConfigValueType INTEGER_TYPE = new IntegerValueType();
    private static final ConfigValueType STRING_TYPE = new StringValueType();
}