package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TTree {
    private class Iterator {
        private TTree target;
        private String[] branchNames;
        public Iterator(TTree target, String[] branchNames) {
            this.target = target;
            this.branchNames = branchNames;
        }

    }

    private Proxy data;
    private ArrayList<TBranch> branches;
    private ArrayList<TLeaf> leaves;
    private TFile file;
    private static final Logger logger = LogManager.getLogger();


    public TTree(Proxy data, TFile file) {
        this.data = data;
        this.file = file;
        branches = new ArrayList<TBranch>();
        leaves = new ArrayList<TLeaf>();
        ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
        for (Proxy val: fBranches) {
            // Drop branches with neither subbranches nor leaves
            TBranch branch = new TBranch(val, this, null);
            if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
                branches.add(branch);
            } else {
                // TODO: would be good to have a "one-off" log4j log
                logger.info("Ignoring unparsable/empty branch \"{}\"", branch.getName());
            }
        }
    }

    public TFile getBackingFile() {
        return file;
    }

    /**
     * Returns either all branches or just the specified branches
     *
     * @param names branches to return
     * @return list of branches specified or all if no branches are specified
     */
    public ArrayList<TBranch> getBranches(String... names) {
        if (names.length == 0) {
            return branches;
        } else {
            ArrayList<TBranch> ret = new ArrayList<TBranch>();
            boolean [] nameFound = new boolean[names.length];
            for (TBranch branch: branches) {
                for (int i = 0; i < names.length; i += 1) {
                    String name = names[i];
                    if (branch.getName().equals(name)) {
                        if (nameFound[i] == false) {
                            nameFound[i] = true;
                            ret.add(branch);
                        } else {
                            logger.error("Duplicate branch found: " + name);
                        }
                    }
                }
            }
            if (ret.size() != names.length) {
                throw new RuntimeException("Could not find all requested branches");
            }
            return ret;
        }
    }

    public ArrayList<TLeaf> getLeaves() {
        return leaves;
    }

    public String getName() {
        return (String) data.getScalar("fName").getVal();
    }

    public long getEntries() {
        return (long) data.getScalar("fEntries").getVal();
    }

    public double[] getIndexValues() {
        return (double []) data.getScalar("fIndexValues").getVal();
    }

    public int[] getIndex() {
        return (int []) data.getScalar("fIndex").getVal();
    }

    public Iterator iterate(String[] branchNames) {
        ProxyArray branches = (ProxyArray) data.getProxy("fBranches");
        for (Proxy val: branches) {
            for (String val2: val.data.keySet()) {
                if (!val2.startsWith("fBasket")) {
                    continue;
                }
            }
        }
        ProxyArray leaves = (ProxyArray) data.getProxy("fLeaves");
        for (Proxy val: leaves) {

        }

        return new Iterator(this, branchNames);
    }
}
