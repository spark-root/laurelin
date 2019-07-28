package edu.vanderbilt.accre;

import static org.junit.Assume.assumeTrue;

import java.io.File;

public class Helpers {
    /**
     * To prevent having an enormous git repo, we try to keep only small
     * (1-10 kB) ROOT files as our test cases. However, incoming bugs will be
     * initially triggered and triaged by "real" files, which are in the MB-GB
     * range. <br />
     *
     * <p>To try and keep the repo size to a minimum, the "pristine" error cases
     * will be stored outside of git, and any test cases that depend on it will
     * be skipped if the file isn't there. These data will be stored under
     * testdata/pristine
     *
     * @param path File name of the desired test data
     * @return The relative path of the data
     */
    public static String getBigTestDataIfExists(String path) {
        String pristinePath = path.replace("testdata/", "testdata/pristine/");
        File f = new File(pristinePath);
        if (f.isFile()) {
            return pristinePath;
        }
        f = new File(path);
        assumeTrue(f.isFile());
        return path;
    }
}
