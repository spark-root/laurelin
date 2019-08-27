package edu.vanderbilt.accre;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage;
import edu.vanderbilt.accre.laurelin.root_proxy.IOProfile.Event.Storage.TypeEnum;

public class Helpers {
    private static final Logger logger = LogManager.getLogger();

    /**
     * A debug-only option to read minified files even if the pristine files
     * are present. This should <b>never</b> be committed as true. If this is
     * enabled, all reads against the minified source will be compared against
     * the pristine source to verify that the content matches.
     */
    public static final boolean overridePristineReads = false;

    /**
     * To prevent having an enormous git repo, we try to keep only small
     * (1-10 kB) ROOT files as our test cases. However, incoming bugs will be
     * initially triggered and triaged by "real" files, which are in the MB-GB
     * range. <br />
     *
     * <p>To try and keep the repo size to a minimum, the "pristine" error cases
     * will be stored outside of git, and any test cases that depend on it will
     * be skipped if the file isn't there. These data will be stored under
     * testdata/pristine.
     *
     * <p>These data could eventually take up quite a significant amount of
     * space, so an additional space-saving measure is minification of the
     * files. This is accomplished by recording a trace of all I/O performed
     * against the test file, then zeroing out all the bytes that are not
     * touched and compressing the result.
     *
     * @param testClass Used to enforce that our custom AfterClass handler is called
     * @param path File name of the desired test data
     * @return The relative path of the data
     */

    public static String getBigTestDataIfExists(LaurelinTest testClass, String path) {
        Function<Event, Integer> cb = null;
        cb = e -> {
            profileStorage.add(e.getStorage());
            return 0;
        };
        IOProfile.getInstance(1, cb);
        String pristinePath = path.replace("testdata/", "testdata/pristine/");
        File p = new File(pristinePath);
        boolean pristineExists = p.isFile();

        String minifiedPath = path.replace("testdata/", "testdata/minified/") + ".xz";
        File m = new File(minifiedPath);
        boolean minifiedExists = m.isFile();

        assumeTrue(pristineExists || minifiedExists);
        if (pristineExists && !overridePristineReads) {
            return pristinePath;
        } else if (minifiedExists) {
            // tell the file I/O subsystem the whole file is xz compressed
            minifiedPath = "$$XZ$$" + minifiedPath;
            return minifiedPath;
        } else if (pristineExists) {
            return pristinePath;
        } else {
            assumeFalse(true);
            return null;
        }
    }

    /**
     * Stores all the I/O for all test cases globally. We use this information
     * to allow us to minify large test cases into smaller ones. Obnoxiously,
     * neither junit nor java have the right support to perform an action right
     * before the JVM shuts down, so users have to manually call checkpointIO
     * after each test class to make sure all the info is properly stored
     */
    public static List<Storage> profileStorage = Collections.synchronizedList(new LinkedList<IOProfile.Event.Storage>());
    public static String profilePath = Paths.get("").toAbsolutePath().toString() + "/unittest_ioprofile.txt";

    public static void checkpointIO() {
        logger.info("Checkpointing IOProfile data to " + profilePath);
        HashMap<Integer, String> fidMap = new HashMap<Integer, String>();
        HashMap<Integer, RangeSet<Long>> rangeMap = new HashMap<Integer, RangeSet<Long>>();
        synchronized (profileStorage) {
            for (IOProfile.Event.Storage stor: profileStorage) {
                if (stor.type == TypeEnum.UPPER) {
                    if ((stor.fileName != null)) {
                        assert ((fidMap.containsKey(stor.fid) == false) || (fidMap.get(stor.fid).equals(stor.fileName)));
                        fidMap.put(stor.fid, stor.fileName);
                        rangeMap.putIfAbsent(stor.fid, TreeRangeSet.create());
                    } else {
                        if (fidMap.get(stor.fid).startsWith("testdata/pristine")) {
                            /*
                             *  Only care to log the IO of the pristine files since
                             *  that's what will be minimized
                             */
                            rangeMap.get(stor.fid).add(Range.closedOpen(stor.offset, stor.offset + stor.len));
                        }
                    }
                }
            }
        }

        // Collect all ranges of the same filename together
        HashMap<String, LinkedList<RangeSet<Long>>> stringToRangeMap = new HashMap<String, LinkedList<RangeSet<Long>>>();
        for (Integer fid: fidMap.keySet()) {
            if (!(fidMap.get(fid).startsWith("testdata/pristine"))) {
                continue;
            }
            stringToRangeMap.putIfAbsent(fidMap.get(fid), new LinkedList<RangeSet<Long>>());
            stringToRangeMap.get(fidMap.get(fid)).add(rangeMap.get(fid));
        }

        // Coalesce the repeated reads of the same bytes in the same files
        HashMap<String, ImmutableRangeSet<Long>> immRangeMap = new HashMap<String, ImmutableRangeSet<Long>>();
        for (String path: stringToRangeMap.keySet()) {
            RangeSet<Long> tmpRange = TreeRangeSet.create();
            for (RangeSet<Long> r: stringToRangeMap.get(path)) {
                tmpRange.addAll(r);
            }
            immRangeMap.put(path, ImmutableRangeSet.unionOf(tmpRange.asRanges()));
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(profilePath));
            for (String path: immRangeMap.keySet()) {
                writer.write("#BEGIN " + path + "\n");
                for (Range<Long> p: immRangeMap.get(path).asDescendingSetOfRanges()) {
                    writer.write(p.lowerEndpoint() + " " + p.upperEndpoint() + "\n");
                }
            }
            writer.write("#EOF");
            writer.close();
        } catch (IOException e) {
            // Swallow this exception since that code is secondary to the main goal
            logger.error("Failed to write IOProfile info: " + e.toString());
        }
    }

}
