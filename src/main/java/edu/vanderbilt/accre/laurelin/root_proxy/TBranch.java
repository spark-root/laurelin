package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TBranch {
    protected Proxy data;
    protected ArrayList<TBranch> branches;
    private ArrayList<TLeaf> leaves;
    private ArrayList<TBasket> baskets;

    protected boolean isBranch;
    protected TBranch parent;
    protected TTree tree;

    public static class ArrayDescriptor implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean isFixed;
        private int fixedLength;
        private String branchName;
        public static ArrayDescriptor newNumArray(String mag) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = true;
            ret.fixedLength = Integer.parseInt(mag);
            return ret;
        }
        public static ArrayDescriptor newVarArray(String mag) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = false;
            ret.branchName = mag;
            return ret;
        }
        public boolean isFixed() {
            return isFixed;
        }
        public int getFixedLength() {
            return fixedLength;
        }
    }

    public TBranch(Proxy data, TTree tree, TBranch parent) {
        this.data = data;
        this.parent = parent;
        this.tree = tree;

        branches = new ArrayList<TBranch>();
        if (getClass().equals(TBranch.class)) {
            baskets = new ArrayList<TBasket>();
            leaves = new ArrayList<TLeaf>();
            isBranch = true;
            ProxyArray fBranches = (ProxyArray) data.getProxy("fBranches");
            for (Proxy val: fBranches) {
                TBranch branch = new TBranch(val, tree, this);
                // Drop branches with neither subbranches nor leaves
                if (branch.getBranches().size() != 0 || branch.getLeaves().size() != 0) {
                    if (branch.getName().startsWith("P3")) {
                        continue;
                    }
                    branches.add(branch);
                }
            }
            ProxyArray fLeaves = (ProxyArray) data.getProxy("fLeaves");
            for (Proxy val: fLeaves) {
                TLeaf leaf = new TLeaf(val, tree, this);
                if (leaf.typeUnhandled()) {
                    continue;
                }
                leaves.add(leaf);
            }

            /*
             * Instead of being in the ObjArray of fBaskets, ROOT stores the baskets in separate
             * toplevel entries in the file
             *     Int_t      *fBasketBytes;      ///<[fMaxBaskets] Length of baskets on file
             *     Long64_t   *fBasketEntry;      ///<[fMaxBaskets] Table of first entry in each basket
             *     Long64_t   *fBasketSeek;       ///<[fMaxBaskets] Addresses of baskets on file
             */
            int fMaxBaskets = (int) data.getScalar("fMaxBaskets").getVal();
            int[] fBasketBytes = (int[]) data.getScalar("fBasketBytes").getVal();
            long[] fBasketEntry = (long[]) data.getScalar("fBasketEntry").getVal();
            long[] fBasketSeek = (long[]) data.getScalar("fBasketSeek").getVal();
            baskets = new ArrayList<TBasket>();
            TFile backing = tree.getBackingFile();
            for (int i = 0; i < fMaxBaskets; i += 1) {
                Cursor c;
                if (fBasketSeek[i] == 0) {
                    // An empty basket?
                    continue;
                }
                try {
                    c = backing.getCursorAt(fBasketSeek[i]);
                    TBasket b = TBasket.getFromFile(c, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i]);
                    baskets.add(b);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            isBranch = false;
        }
    }

    public TTree getTree() {
        return tree;
    }

    public String getTitle() {
        return (String) data.getScalar("fTitle").getVal();
    }

    public String getName() {
        return (String) data.getScalar("fName").getVal();
    }

    public String getFullName() {
        if (parent == null) {
            return (String) data.getScalar("fName").getVal();
        } else {
            TBranch tmpParent = parent;
            String ret = "." + getName();
            while (tmpParent != null) {
                ret = tmpParent.getName() + ret;
                tmpParent = tmpParent.parent;
                if (tmpParent != null) {
                    ret = "." + ret;
                }
            }
            return ret;
        }
    }

    public String getClassName() {
        return data.getClassName();
    }

    public ArrayList<TBranch> getBranches() {
        return branches;
    }

    public List<TLeaf> getLeaves() {
        return leaves;
    }

    /**
     * returns fType
     * @return ROOT type integer constant
     */
    public Integer getType() {
        return (Integer) data.getScalar("fType").getVal();
    }

    /**
     * returns normalized fType
     * @return ROOT type integer constant
     */
    public Integer getNormedType() {
        Integer fType = (Integer) data.getScalar("fType").getVal();
        if ((Constants.kOffsetL < fType) && (fType < Constants.kOffsetP)) {
            fType = fType - Constants.kOffsetL;
        }
        return fType;
    }

    public List<TBasket> getBaskets() {
        return baskets;
    }

    /*
     * Note: this will all have to be refactored to support multidimensional
     *       arrays, but sufficient is today for its own troubles
     */

    // really wish java had a non-escaped string specifier
    // I wanna grab everything out of the square brackets
    //                                          \[(\d+)\]
    Pattern arrayNumPattern = Pattern.compile("\\[(\\d+)\\]");
    //                                          \[([^\]]+)\]
    Pattern arrayVarPattern = Pattern.compile("\\[([^\\]]+)\\]");

    /**
     * Returns an ArrayDescriptor describing this branch
     *
     * @return ArrayDescriptor containing the array params if this is an array
     *         null otherwise
     */
    public ArrayDescriptor getArrayDescriptor() {
        if (getLeaves().size() != 1) {
            throw new RuntimeException("Non-split branches are not supported");
        }
        TLeaf leaf = getLeaves().get(0);
        String title = leaf.getTitle();
        if (!title.contains("[")) {
            // no square brackets means no possibility of being an array
            return null;
        } else if (title.indexOf("[") != title.lastIndexOf(("["))) {
            throw new RuntimeException("Multidimensional arrays are not supported");
        } else {
            Matcher numMatcher = arrayNumPattern.matcher(title);
            Matcher varMatcher = arrayVarPattern.matcher(title);
            if (numMatcher.find()) {
                return ArrayDescriptor.newNumArray(numMatcher.group(1));
            } else if (varMatcher.find()) {
                return ArrayDescriptor.newVarArray(varMatcher.group(1));
            } else {
                throw new RuntimeException("Unable to parse array indices");
            }
        }
    }

    public boolean typeUnhandled() { return false; }

    public SimpleType getSimpleType() {
        SimpleType ret = null;
        if (leaves.size() == 1){
            TLeaf leaf = leaves.get(0);
            if (getTitle().length() >= 2) {
                ret = getTypeFromTitle(getTitle());
            }

            if (ret == null) {
                ret = leaf.getLeafType();
            }
        }
        if (ret == null) {
            throw new RuntimeException("Unknown simple type for branch named: " + this.getName());
        }

        return ret;
    }

    protected SimpleType getTypeFromTitle(String title) {
        SimpleType ret = null;
        String lastTwo = title.substring(title.length() - 2, title.length());
        if (lastTwo.charAt(0) == '/') {
            switch (lastTwo) {
                case ("/B"):
                    ret = SimpleType.Int8;
                    break;
                case ("/b"):
                    ret = SimpleType.UInt8;
                    break;
                case ("/S"):
                    ret = SimpleType.Int16;
                    break;
                case ("/s"):
                    ret = SimpleType.UInt16;
                    break;
                case ("/I"):
                    ret = SimpleType.Int32;
                    break;
                case ("/i"):
                    ret = SimpleType.UInt32;
                    break;
                case ("/L"):
                    ret = SimpleType.Int64;
                    break;
                case ("/l"):
                    ret = SimpleType.UInt64;
                    break;
                case ("/O"):
                    ret = SimpleType.Bool;
                    break;
                case ("/F"):
                    ret = SimpleType.Float32;
                    break;
                case ("/D"):
                    ret = SimpleType.Float64;
                    break;
                default:
                    throw new RuntimeException("Unknown branch type: " + lastTwo + " name is: " + title);
            }
            // Do I later want to separate fixed and not-fixed arrays?
            if (title.contains("[")) {
                ret = new SimpleType.ArrayType(ret);
            }
        }
        return ret;
    }

    public long[] getBasketEntryOffsets() {
        int basketCount = baskets.size();
        // The array processing code wants a final entry to cap the last true
        // basket from above
        long []ret = new long[basketCount + 1];
        for (int i = 0; i < basketCount; i += 1) {
            ret[i] = baskets.get(i).getBasketEntry();
        }
        ret[basketCount] = tree.getEntries();
        return ret;
    }

}
