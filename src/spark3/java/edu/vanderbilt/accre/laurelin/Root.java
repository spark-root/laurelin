package edu.vanderbilt.accre.laurelin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.vanderbilt.accre.laurelin.adaptor_v30.Root_v30;

/**
 * Toplevel Version-agnostic Spark entrypoint
 * <p>
 * This is backed by Root_v30 and Root_v24, depending on the extends statement
 * <p>
 * There's no real good way in java to autodetect the running spark version at
 * class loading time, so the "default" version is hardcoded by "extends". Users
 * can manually choose to use any of the backends if they'd like to, though, by
 * using (e.g.) .format("root_v24") instead of .format("root")
 */
public class Root extends Root_v30 {
    static final Logger logger = LogManager.getLogger();

    @Override
    public String shortName() {
        return "root";
    }
}