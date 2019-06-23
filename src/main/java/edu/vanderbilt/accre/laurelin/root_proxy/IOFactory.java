/**
 * Factory object to load concrete IO implementations based on path
 */

package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class IOFactory {
    static final String hadoopPattern = "^[a-zA-Z]+:/.*";

    public static FileInterface openForRead(String path) throws IOException {
        /**
         * Depending on the incoming path, load an implementation
         */
        if (Pattern.matches(hadoopPattern, path)) {
            return new HadoopFile(path);
        } else {
            return new NIOFile(path);
        }
    }

    public static List<String> expandPathToList(String path) throws IOException {
        if (Pattern.matches(hadoopPattern,  path)) {
            return HadoopFile.expandPathToList(path);
        } else {
            return NIOFile.expandPathToList(path);
        }
    }
}
