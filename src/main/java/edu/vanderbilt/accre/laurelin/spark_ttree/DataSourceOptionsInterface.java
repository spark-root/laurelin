package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.List;

public interface DataSourceOptionsInterface {

    String getOrDefault(String key, String defVal);

    int getInt(String string, int i);

    List<String> getPaths();

}
