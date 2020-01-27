package edu.vanderbilt.accre.laurelin.configuration;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Stores all known configuration values, used both at runtime and to generate
 * documentation. This ConfigListing is conceptually a list of ConfigOption each
 * of which handles the processing of a single key/value configuration pair.
 * Importantly, Spark only stores/outputs strings, so we'll do the same here.
 */
public class ConfigListing {
    /**
     * List of ConfigOptions, each of which describes one possible configuration
     * option
     */
    private ConfigOption[] config;

    /**
     * Constructs a new ConfigListing from a list of ConfigOptions
     * @param opts
     */
    public ConfigListing(ConfigOption[] opts) {
        this.config = opts;
    }

    /**
     * Static function returning the default ConfigListing for spark
     * @return Sparks default config listing
     */
    public static ConfigListing getDefaultListing() {
        return new ConfigListing(SparkOptions.getConfigList());
    }

    /**
     * Given a key and a config, return the resolved configuration value. Note
     * that this is always a string, the user (typically LaurelinDSConfig) are
     * responsible for performing the casting.
     * @param key The desired configuratin option
     * @param userConfig The asMap from the DataSourceOptions value
     * @return The fully-resolved configuration value
     */
    public String resolveValue(String key, Map<String, String> userConfig) {
        Optional<String> val = Optional.ofNullable(userConfig.get(key));
        ConfigOption optDefinition = getConfigOption(key);
        return optDefinition.getValue(val, userConfig, this);
    }

    /**
     * ConfigOption corresponding to a given key.
     * @param key
     * @return
     */
    public ConfigOption getConfigOption(String key) {
        for (ConfigOption c: config) {
            // Spark lowercases all configuration keys
            if (key.toLowerCase().equals(c.getName().toLowerCase())) {
                return c;
            }
        }
        throw new NoSuchElementException("Unknown config value: " + key);
    }

    /**
     * Ensure that all the user-provided configuration values exist. Throw an
     * exception if they've provided values that we're unaware of. This helps
     * the case where a typo in a configuration name goes silently unnoticed
     *
     * @param map values from a spark DataSourceOption
     */
    public void validateConfigMap(Map<String, String> map) {
        // Very simple test for now..
        for (String k: map.keySet()) {
            getConfigOption(k);
        }
    }

    /**
     * Represents a configuration value type
     */
    public abstract static class ConfigValueType {
        /**
         * Validates the user-provided parameter against (possibly custom) rules
         * @param val The parameter to validate
         * @return Error message if failed, an empty optional otherwise
         */
        abstract Optional<String> validate(String val);
    }

    /**
     * Ensures the stored value is an integer
     */
    public static class IntegerValueType extends ConfigValueType {
        @Override
        Optional<String> validate(String val) {
            try {
                Integer.parseInt(val);
                return Optional.empty();
            } catch (RuntimeException e) {
                return Optional.of("Could not convert " + val + " to an integer");
            }
        }
    }

    /**
     * Ensures the stored value is a long
     */
    public static class LongValueType extends ConfigValueType {
        @Override
        Optional<String> validate(String val) {
            try {
                Long.parseLong(val);
                return Optional.empty();
            } catch (RuntimeException e) {
                return Optional.of("Could not convert " + val + " to an integer");
            }
        }
    }

    /**
     * Ensures the stored value is a string. This is trivially true since we get
     * Strings from the Spark config map
     */
    static class StringValueType extends ConfigValueType {
        @Override
        Optional<String> validate(String val) {
            // No way a string can't be valid
            return Optional.empty();
        }
    }

    /**
     * Represents default values. Typically, this is stored literally in the
     * configuration, but other possibilities exist. A DependentDefault sets
     * the default of this config to the value of another. This is useful in
     * cases where it's possible to set the variable differently in driver and
     * executor contexts
     */
    private abstract static class ConfigDefault {
        public abstract String getDefault(String key, Map<String, String> userConfig, ConfigListing configListing);
    }

    private static class LiteralDefault extends ConfigDefault {
        String def;

        public LiteralDefault(String def) {
            this.def = def;
        }

        @Override
        public String getDefault(String key, Map<String, String> userConfig, ConfigListing configListing) {
            return def;
        }
    }

    private static class DependentDefault extends ConfigDefault {
        String other;

        public DependentDefault(String other) {
            this.other = other;
        }

        @Override
        public String getDefault(String key, Map<String, String> userConfig, ConfigListing configListing) {
            return configListing.resolveValue(other, userConfig);
        }
    }

    /**
     * Static helper function to construct ConfigBuilder objects
     * @param name Name of the desired ConfigOption
     * @return A new ConfigBuilder
     */
    public static ConfigBuilder newConfig(String name) {
        return new ConfigBuilder(name);
    }

    /**
     * Builder class for ConfigOption objects that allows chaining like
     * <pre>{@code
     *  newConfig("tree")
     *      .type(STRING_TYPE)
     *      .literalDefault("Events")
     *      .description("The name of the TTree to load from our file(s)")
     *      .build()
     * }</pre>
     */
    public static class ConfigBuilder {
        private String name;
        private String desc;
        private ConfigValueType type = null;
        private ConfigDefault def = null;

        public ConfigBuilder(String name) {
            this.name = name;
        }

        /**
         * Sets the description
         * @param desc desired description
         * @return Chainable ConfigBuilder
         */
        public ConfigBuilder description(String desc) {
            this.desc = desc;
            return this;
        }

        /**
         * Sets the type
         * @param type the desired ConfigValueType describing the type of this option
         * @return Chainable ConfigBuilder
         */
        public ConfigBuilder type(ConfigValueType type) {
            this.type = type;
            return this;
        }

        /**
         * Sets the default to a literal value
         * @param def The desired default value
         * @return Chainable ConfigBuilder
         */
        public ConfigBuilder literalDefault(String def) {
            this.def = new LiteralDefault(def);
            return this;
        }

        /**
         * Sets the default to be dependent on another option
         * @param other Name of the option whose value we should use
         * @return Chainable ConfigBuilder
         */
        public ConfigBuilder dependentDefault(String other) {
            this.def = new DependentDefault(other);
            return this;
        }

        /**
         * Instantiates a ConfigOption with the options this builder has built
         * up
         * @return A new ConfigOption described by this builder
         */
        public ConfigOption build() {
            return new ConfigOption(name, type, def, desc);
        }
    }

    /**
     * Stores the description for a single configuration option
     *
     */
    public static class ConfigOption {
        /**
         * Plaintext description
         */
        private String desc;
        /**
         * Name of this option
         */
        private String name;
        /**
         * Type of this option, subclasses of ConfigValueType can also have
         * custom validation logic
         */
        private ConfigValueType type;
        /**
         * Default value of this option. Can either be a literal or a custom
         * implementation that loads the value from another option
         */
        private ConfigDefault defValue;

        /**
         * Private constructor, used by ConfigBuilder
         * @param name Name
         * @param type Type
         * @param defValue Default value
         * @param desc Plaintext description
         */
        private ConfigOption(String name, ConfigValueType type, ConfigDefault defValue, String desc) {
            this.name = name;
            this.type = type;
            this.defValue = defValue;
            this.desc = desc;
        }

        /**
         * Accessor for this option's name
         * @return name of this option
         */
        public String getName() {
            return name;
        }

        /**
         * Description of this option
         * @return plaintext description
         */
        public String getDescription() {
            return desc;
        }

        /**
         * Given an Optional string value, return the sanitized version based
         * on this ConfigOptions parameters
         *
         * @param optionalVal User-supplied value
         * @param configListing
         * @param userConfig
         * @return Sanitized config string
         */
        private String getValue(Optional<String> optionalVal, Map<String, String> userConfig, ConfigListing configListing) {
            if (!optionalVal.isPresent()) {
                if (defValue == null) {
                    throw new RuntimeException("Configuration value " + name + " was requested, but the value was unset by the user and no default was provided");
                } else {
                    // Default values can occasionally be other values, so we need
                    // to pass in the whole state
                    optionalVal = Optional.of(defValue.getDefault(name, userConfig, configListing));
                }
            }
            String val = optionalVal.get();

            Optional<String> validationError = Optional.empty();
            if (type != null) {
                validationError = type.validate(val);
                if (validationError.isPresent()) {
                    throw new RuntimeException("Error processing " + name + ": " + validationError.get());
                }
            }
            return val;
        }
    }
}