package edu.vanderbilt.accre.laurelin.spark_ttree;

public interface DataSourceOptionsInterface {

    String getOrDefault(String key, String defVal);

    int getInt(String string, int i);

}
