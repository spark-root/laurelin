/**
 * Factory object to load concrete IO implementations based on path
 */

package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class IOFactory {
    static final String hadoopPattern = "^[a-zA-Z]+:.*";

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

    /**
     * Perform glob-expansion on a list of paths, then recursively expand any
     * directories listed in the list.
     *
     * @param paths Paths to be expanded
     * @return Fully expanded list of ROOT file paths
     * @throws IOException If any globs don't resolve or paths don't exist
     */
    public static List<Path> resolvePathList(List<String> paths) throws IOException {
        Configuration hadoopConf;
        try {
            hadoopConf = SparkSession.active().sparkContext().hadoopConfiguration();
        } catch (IllegalStateException e) {
            hadoopConf = new Configuration();
        }

        List<Path> globResolved = new ArrayList<Path>(paths.size());
        // First perform any globbing
        for (String path: paths) {
            if (isGlob(path)) {
                globResolved.addAll(resolveGlob(path));
            } else {
                globResolved.add(new Path(path));
            }
        }

        /*
         * Now, with globs turned into concrete paths, we want to walk through
         * the list and check the type of each file:
         *
         * 1) If a file, add that file directly to our list of input paths
         * 2) If a directory, recurseivly add every file ending in .root
         *
         * There is a problem, however. Each file lookup is synchronous, and if
         * the filesystem is remote (e.g. reading xrootd across the WAN), each
         * stat() can take upwards of 100msec, which can take forever if the
         * user passes in a list of 10k files they'd like to process.
         *
         * As an optimization, instead of requesting the status of each path
         * directly, request the directory listing of each path's parent
         * directory to discover the types of each entry. This way, the number
         * of FS calls scales by the number of parent directories and not the
         * number of paths.
         *
         * It should also be noted that the hadoop-xrootd connector unrolls
         * the multi-arg form of listStatus to individual calls, so that doesn't
         * help.
         */

        // Loop over all the paths and keep the unique parents of them all
        // TODO: Is repeatedly instantiating FileSystem objects slow over WAN?
        Map<Path, List<FileStatus>> parentDirectories = new HashMap<Path, List<FileStatus>>();
        Map<Path, Path> childToParentMap = new HashMap<Path, Path>();
        Map<Path, Path> qualifiedChildToParentMap = new HashMap<Path, Path>();
        for (Path path: globResolved) {
            Path parent = path.getParent();
            parentDirectories.put(parent, null);
            childToParentMap.put(path, parent);
            FileSystem fs = parent.getFileSystem(hadoopConf);
            Path qualifiedChild = path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
            qualifiedChildToParentMap.put(qualifiedChild, parent);
        }

        // Retrieve the listing for all the parent dirs
        Map<Path, List<FileStatus>> parentToStatusMap = new HashMap<Path, List<FileStatus>>();
        Map<Path, FileStatus> qualifiedListingToStatusMap = new HashMap<Path, FileStatus>();
        for (Path parent: parentDirectories.keySet()) {
            FileSystem fs = parent.getFileSystem(hadoopConf);
            FileStatus[] listing = fs.listStatus(parent);
            parentToStatusMap.put(parent, Arrays.asList(listing));
            for (FileStatus s: listing) {
                assert qualifiedListingToStatusMap.containsKey(s.getPath()) == false;
                qualifiedListingToStatusMap.put(s.getPath(), s);
            }
        }

        assert qualifiedListingToStatusMap.size() >= globResolved.size(): "qualifiedlisting < globresolved";

        /*
         *  At this point, we have a list of post-globbing URIs and lists of
         *  FileStatus for every parent of those URIs. Use this to make a map of
         *  Globbed path -> FileStatus
         */
        Map<Path, FileStatus> clientRequestedPathToStatusMap = new HashMap<Path, FileStatus>();
        for (Entry<Path, Path> e: qualifiedChildToParentMap.entrySet()) {
            if (!qualifiedListingToStatusMap.containsKey(e.getKey())) {
                throw new IOException("Path not found: " + e.getKey());
            }
            FileStatus status = qualifiedListingToStatusMap.get(e.getKey());
            clientRequestedPathToStatusMap.put(e.getKey(), status);
        }

        // Walk the statuses to sort between files and directories
        List<Path> ret = new ArrayList<Path>(globResolved.size());
        for (FileStatus status: clientRequestedPathToStatusMap.values()) {
            Path path = status.getPath();
            if (status.isDirectory()) {
                // We were given a directory, add everything recursively
                FileSystem fs = status.getPath().getFileSystem(hadoopConf);
                RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(status.getPath(), true);
                while (fileList.hasNext()) {
                    LocatedFileStatus file = fileList.next();
                    if (file.isFile() && (file.getPath().getName().endsWith(".root"))) {
                        ret.add(file.getPath());
                    }
                }
            } else if (status.isFile()) {
                ret.add(status.getPath());
            } else {
                throw new IOException("File '" + path + "' is an unknown type");
            }
        }

        return ret;
    }

    /**
     * Perform glob expansion on a path
     * @param path Glob to expand
     * @return List of paths that match the given glob
     * @throws IOException Nothing matches the given glob
     */
    private static List<Path> resolveGlob(String path) throws IOException {
        Configuration hadoopConf;
        try {
            hadoopConf = SparkSession.active().sparkContext().hadoopConfiguration();
        } catch (IllegalStateException e) {
            hadoopConf = new Configuration();
        }

        Path hdfsPath = new Path(path);
        FileSystem fs = hdfsPath.getFileSystem(hadoopConf);
        Path qualified = hdfsPath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
        Seq<Path> globPath = SparkHadoopUtil.get().globPathIfNecessary(fs, qualified);
        if (globPath.isEmpty()) {
            throw new IOException("Path does not exist: " + qualified);
        }
        // TODO: Is this stable across Scala versions?
        List<Path> ret = JavaConverters.seqAsJavaListConverter(globPath).asJava();
        return ret;
    }

    /**
     * See if the given path has any glob metacharacters
     * @param path Input path
     * @return True if the path looks like a glob. False otherwise.
     */
    private static boolean isGlob(String path) {
        return path.matches(".*[{}\\[\\]*?].*");
    }

    public static List<String> expandPathToList(String path) throws IOException {
        if (Pattern.matches(hadoopPattern,  path)) {
            return HadoopFile.expandPathToList(path);
        } else {
            return NIOFile.expandPathToList(path);
        }
    }
}
