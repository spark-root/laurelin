package edu.vanderbilt.accre;

import org.junit.AfterClass;

public class LaurelinTest {
    /**
     * We need to trace ALL io to our testcase files so we can minify multi-GB
     * files. This helper will flush out our global profile information after
     * each test class to a file, so we can run post-processing scripts to
     * zero-out and then xz compress our large test cases (xz has the nice
     * property that it's seekable without having to decompress the whole file)
     */
    @AfterClass
    public static void checkpointIO() {
        Helpers.checkpointIO();
    }
}
