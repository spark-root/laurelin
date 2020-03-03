/**
 * Factory object to load concrete IO implementations based on path
 */

package edu.vanderbilt.accre.laurelin.root_proxy.io;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import edu.vanderbilt.accre.laurelin.root_proxy.XZDecompressionWrapper;

public class IOFactory {
    static final String hadoopPattern = "^[a-zA-Z]+:/.*";

    public static FileInterface openForRead(String path) throws IOException {
        /**
         * Depending on the incoming path, load an implementation
         */

        /*
         *  We dedup and shrink debug data by filling unread parts with zeros
         *  and then compressing the file. This data needs to be wrapped in an
         *  xz decompressor to load everything
         */
        FileInterface ret;
        if (path.startsWith("$$XZ$$")) {
            //Only support reading xz-compressed files locally
            path = path.replace("$$XZ$$", "");
            ret = new XZDecompressionWrapper(path);

        } else if (Pattern.matches(hadoopPattern, path)) {
            ret = new HadoopFile(path);
        } else {
            ret = new NIOFile(path);
        }

        return ret;
    }

    public static List<String> expandPathToList(String path) throws IOException {
        if (Pattern.matches(hadoopPattern,  path)) {
            return HadoopFile.expandPathToList(path);
        } else {
            return NIOFile.expandPathToList(path);
        }
    }
}
