package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.Range;

import edu.vanderbilt.accre.laurelin.root_proxy.io.Constants;
import edu.vanderbilt.accre.laurelin.root_proxy.io.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.serialization.Proxy;
import edu.vanderbilt.accre.laurelin.root_proxy.serialization.ProxyArray;

public class TBranch {
    protected static class EmbeddedBasketsInfo {

		long [] seek;
        Long [][] compressedInfo;
        Long [][] addlLongInfo;
        Integer [][] addlIntInfo;
        int [] fNevBuf;
        int [] basketBytes;
        public EmbeddedBasketsInfo(long[] seek, Long[][] compressedInfo, int[] fNevBuf, int[] basketBytes, Long[][] addlLongInfo, Integer[][] addlIntInfo) {
            this.seek = seek;
            this.compressedInfo = compressedInfo;
            this.fNevBuf = fNevBuf;
            this.basketBytes = basketBytes;
            this.addlLongInfo = addlLongInfo;
            this.addlIntInfo = addlIntInfo;
        }
        public long getSeek(int i) {
            return seek[i];
        }
        public Long[] getCompressedInfo(int i) {
            return compressedInfo[i];
        }
        public int getfNevBuf(int i) {
            return fNevBuf[i];
        }
        public int getBasketBytes(int i) {
            return basketBytes[i];
        }
        public Long[] getAddlLongInfo(int i) {
			return addlLongInfo[i];
		}
		public Integer[] getAddlIntInfo(int i) {
			return addlIntInfo[i];
		}
        public int size() {
            if (seek == null) {
                return 0;
            } else {
                return seek.length;
            }
        }

    }

    private static final Logger logger = LogManager.getLogger();
    protected Proxy data;
    protected ArrayList<TBranch> branches;
    private ArrayList<TLeaf> leaves;
    private ArrayList<TBasket> lazyBasketStorage;
    private int fMaxBaskets = 0;
    private int[] fBasketBytes;
    private long[] fBasketEntry;
    private long[] fBasketSeek;
    private CompressedBasketInfo[] compressedBasketInfo;

    protected boolean isBranch;
    protected TBranch parent;
    protected TTree tree;

    public static class ArrayDescriptor implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean isFixed;
        private int fixedLength;
        private String branchName;
        private int skipBytes;

        public static ArrayDescriptor newNumArray(String mag, int skipBytes) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = true;
            ret.fixedLength = Integer.parseInt(mag);
            ret.skipBytes = skipBytes;
            return ret;
        }

        public static ArrayDescriptor newNumArray(String mag) {
            return newNumArray(mag, 0);
        }

        public static ArrayDescriptor newVarArray(String mag, int skipBytes) {
            ArrayDescriptor ret = new ArrayDescriptor();
            ret.isFixed = false;
            ret.branchName = mag;
            ret.skipBytes = skipBytes;
            return ret;
        }

        public static ArrayDescriptor newVarArray(String mag) {
            return newVarArray(mag, 0);
        }

        public boolean isFixed() {
            return isFixed;
        }

        public int getFixedLength() {
            return fixedLength;
        }

        public int getSkipBytes() {
            return skipBytes;
        }
    }

    public static class CompressedBasketInfo implements Serializable {
        /**
		 * Class version
		 */
		private static final long serialVersionUID = 1L;
		/*
         * Length of the header at the beginning of a compressed range
         */
        int headerLen;
        /*
         * Length of the compressed range
         */
        int compressedLen;
        /*
         * Length of the compressed range after decompression
         */
        int uncompressedLen;
        /*
         * Offset to the first byte of this compressed range
         */
        long compressedParentOffset;
        /*
         * Length of the basket content
         */
        int basketLen;

        long startOffset;
        long basketOffset;
        long finalOffset;
        int keyLen;
        int bufferSize;
        int nevBufSize;
        int nevBuf;
        int last;
        /**
         * @param headerLen
         * @param compressedLen
         * @param uncompressedLen
         * @param compressedParentOffset
         * @param i
         */
        public CompressedBasketInfo(int headerLen, int compressedLen, int uncompressedLen,
                long compressedParentOffset, int basketLen, long startOffset, long basketOffset, long finalOffset,
                int keyLen, int bufferSize, int nevBufSize, int nevBuf, int last) {
            super();
            this.headerLen = headerLen;
            this.compressedLen = compressedLen;
            this.uncompressedLen = uncompressedLen;
            this.compressedParentOffset = compressedParentOffset;
            this.basketLen = basketLen;
            this.startOffset = startOffset;
            this.basketOffset = basketOffset;
            this.finalOffset = finalOffset;
            this.keyLen = keyLen;
            this.bufferSize = bufferSize;
            this.nevBufSize = nevBufSize;
            this.nevBuf = nevBuf;
            this.last = last;
        }
        /**
         *
         * @return the basketLen
         */
        public int getBasketLen() {
        	return basketLen;
        }
        /**
         * @return the headerLen
         */
        public int getHeaderLen() {
            return headerLen;
        }
        /**
         * @return the compressedLen
         */
        public int getCompressedLen() {
            return compressedLen;
        }
        /**
         * @return the uncompressedLen
         */
        public int getUncompressedLen() {
            return uncompressedLen;
        }
        /**
         * @return the compressedParentOffset
         */
        public long getCompressedParentOffset() {
            return compressedParentOffset;
        }
        @Override
        public String toString() {
            return "CompressedBasketInfo [headerLen=" + headerLen + ", compressedLen=" + compressedLen
                    + ", uncompressedLen=" + uncompressedLen + ", compressedParentOffset=" + compressedParentOffset
                    + ", basketLen=" + basketLen
                    + "]";
        }
		public long getStartOffset() {
			return startOffset;
		}
		public long getBasketOffset() {
			return basketOffset;
		}
		public long getFinalOffset() {
			return finalOffset;
		}
		public int getKeyLen() {
			return keyLen;
		}
		public int getBufferSize() {
			return bufferSize;
		}
		public int getNevBufSize() {
			return nevBufSize;
		}
		public int getNevBuf() {
			return nevBuf;
		}
		public int getLast() {
			return last;
		}

    }

    /**
     * Makes an empty TBranch to test internal functions.
     *
     * @return An empty TBranch
     */
    protected static TBranch makeBlankTestTBranch() {
    	return new TBranch();
    }

    /**
     * Protected method, used to construct a fake TBranch.
     */
    private TBranch() {

    }

    public TBranch(Proxy data, TTree tree, TBranch parent) {
        this.data = data;
        this.parent = parent;
        this.tree = tree;
        if (((String) data.getScalar("fName").getVal()).equals("eta")) {
            System.out.println(data.dumpStr());
        }
        branches = new ArrayList<TBranch>();


        if (getClass().equals(TBranch.class)) {
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
            long fEntries =  (long) data.getScalar("fEntries").getVal();
            String fName =  (String) data.getScalar("fName").getVal();
            int fMaxBasketsTmp = (int) data.getScalar("fMaxBaskets").getVal();
            int[] fBasketBytesTmp = (int[]) data.getScalar("fBasketBytes").getVal();
            long[] fBasketEntryTmp = (long[]) data.getScalar("fBasketEntry").getVal();
            long[] fBasketSeekTmp = (long[]) data.getScalar("fBasketSeek").getVal();
            ProxyArray fBaskets = (ProxyArray) data.getProxy("fBaskets");

            EmbeddedBasketsInfo embeddedBaskets;
            if (fBaskets.size() > 0) {
                // Swizzle embedded baskets
                int size = fBaskets.size();
                long [] seek = new long[size];
                Long [][] compressedInfo = new Long[size][];
                Long [][] addlLongInfo = new Long[size][];
                Integer [][] addlIntInfo = new Integer[size][];
                int [] fNevBuf = new int[size];
                int [] laurelinBasketBytes = new int[size];
                for (int i = 0; i < size; i++) {
                    Proxy basket = fBaskets.at(i);
                    seek[i] = (long) basket.getScalar("laurelinBasketOffset").getVal();
                    compressedInfo[i] = (Long []) basket.getScalar("laurelinBasketCompressedInfo").getVal();
                    addlLongInfo[i] = (Long []) basket.getScalar("laurelinAdditionalLongInfo").getVal();
                    addlIntInfo[i] = (Integer []) basket.getScalar("laurelinAdditionalIntInfo").getVal();
                    fNevBuf[i] = (int) basket.getScalar("fNevBuf").getVal();
                    laurelinBasketBytes[i] = (int) basket.getScalar("laurelinBasketBytes").getVal();

                    logger.trace(" Embedded swizzle: " + seek[i] + " : " + Arrays.toString(compressedInfo[i])
                    				+ " : " + fNevBuf[i] + " : " + laurelinBasketBytes[i]
                    				+ " : " + Arrays.toString(addlLongInfo[i])
                    				+ " : " + Arrays.toString(addlIntInfo[i]));
                }
                embeddedBaskets = new EmbeddedBasketsInfo(seek, compressedInfo, fNevBuf, laurelinBasketBytes, addlLongInfo, addlIntInfo);
            } else {
                embeddedBaskets = null;
            }
            int fWriteBasket = (int) data.getScalar("fWriteBasket").getVal();
            logger.trace(fEntries);
            logger.trace(fMaxBasketsTmp);
            logger.trace(Arrays.toString(fBasketBytesTmp));
            logger.trace(Arrays.toString(fBasketEntryTmp));
            logger.trace(Arrays.toString(fBasketSeekTmp));
            logger.trace(fWriteBasket);
            logger.trace("Getting branch " + fName + " fEntries: " + fEntries + " maxBaskets " + fMaxBasketsTmp);
            logger.trace("     bytes      entry      seek       ");
            extractBranchInfo(fEntries, fName, fBasketBytesTmp, fBasketEntryTmp, fBasketSeekTmp, embeddedBaskets,
                    fWriteBasket, fMaxBasketsTmp);
        } else {
            isBranch = false;
        }
    }

    protected void extractBranchInfo(long fEntries, String fName, int[] fBasketBytesTmp, long[] fBasketEntryTmp,
            long[] fBasketSeekTmp, EmbeddedBasketsInfo fBaskets, int fWriteBasket, int fMaxBasketsTmp) {
        for (int i = 0; i < fBasketEntryTmp.length; i += 1) {
            logger.trace(String.format("%04d %10d %10d %10d", i, fBasketBytesTmp[i], fBasketEntryTmp[i], fBasketSeekTmp[i]));
        }

        /*
         *  First, count all the baskets up because we need to re-allocate
         *  new arrays with the correct length. There's two steps,
         *
         *  1) Root sometimes makes zero-length/empty baskets, so we need to
         *  trim them to preserve the invariant in ArrayBuilder that the
         *  values are monotonically increasing, also ensure that we don't
         *  try to deserialize the basket that was being written to, since
         *  it's bound to be incomplete.
         *
         *  Now, the last baskets in a recovery tree looks like this
         *    id      bytes      entry      seek
         *  0007      28200      55860     681573
         *  0008     225179      63832    1506672
         *  0009          0     127664          0
         *
         *  Somewhat frustratingly, loose baskets store the first entry
         *  in the TBranch metadata, but embedded baskets store the
         *  number of entries in the basket, so we have to first walk
         *  the loose ones, find out the last entry of that, then add the
         *  subsequent embedded basket entry counts to get the mapping
         *  between entry and basket.
         *
         *
         */
        long lastEntry = 0;
        // Count the valid loose baskets
        int looseBaskets = 0;
        for (int i = 0; i < fMaxBasketsTmp; i += 1) {
//                boolean topEdgeBasket = false;
//                if ((i > 0) && (fBasketSeekTmp[i] == 0) && (fBasketEntryTmp[i] != 0) && (fBasketBytesTmp[i] == 0)) {
//                    topEdgeBasket = true;
//                }
            if (((fBasketSeekTmp[i] != 0) && (i != fWriteBasket))) {
                // this is a basket worth deserializing
                looseBaskets += 1;
                lastEntry = fBasketEntryTmp[i];
            } else {
                logger.trace("  found else basket at index " + i + " entry " + fBasketEntryTmp[i]);
                /*
                 * We want the final basket that's not the write basket.
                 *
                 * This is basket ID 9 from the example above. When ROOT
                 * writes out the basket metadata, it always puts a blank
                 * basket past the last valid basket to let you know where
                 * the previous basket ended. In this case, you know that
                 * basket 8 started at 63832 and ended at (127664 -1)
                 *
                 */
                lastEntry = fBasketEntryTmp[i];
            }
            logger.trace("  last loose basket entry is " + lastEntry + " inc is " + fBasketEntryTmp[i]);
        }
        /*
         * At this point, we've counted all the valid baskets and
         * lastEntry is one entry past the final loose entry.
         *
         * 2) Process baskets from not-cleanly-closed TTrees. These are the
         * files that ROOT will say "recovery in progress" to.
         */
        int embeddedBaskets = 0;
        if (fEntries == fBasketEntryTmp[looseBaskets]) {
            /*
             *  All the baskets in the "regular" location add up to the
             *  number of entries the TBranch purports to have, so we know
             *  there are no "embedded" baskets to search for
             */
        } else if ((fBaskets != null) && (fBaskets.size() > 0)) {
            embeddedBaskets = fBaskets.size();
        }

        /*
         * Now, make a unified way to access these baskets. The on-disk
         * format for the baskets is the same if it's loose or embedded,
         * so just point the later processing to the "top" of all baskets,
         * regardless of where they're stored on disk.
         *
         * NOTE: the "+1" is because the ROOT convention is to only list
         * the first entry in the basket and not the number of entries in
         * each basket. To specify the number amounts in the final basket,
         * ROOT adds a final "blank" basket at the end with the "first"
         * entry in that basket. The number of entries in the final basket
         * is then (n_blank - n_final). Let's hold the invariant by adding
         * our own fake final entry, which takes into account any embedded
         * branches, which wouldn't show up at all in the original
         * fBasketEntry, etc.. arrays.
         */

        int correctedBasketCount = looseBaskets + embeddedBaskets + 1;
        fBasketBytes = new int[correctedBasketCount];
        fBasketEntry = new long[correctedBasketCount];
        fBasketSeek = new long[correctedBasketCount];
        compressedBasketInfo = new CompressedBasketInfo[correctedBasketCount];
        int j = 0;
        // Loose baskets are the easy case, the TBranch gives these vals
        for (int i = 0; i < looseBaskets; i += 1) {
            fBasketBytes[j] = fBasketBytesTmp[i];
            fBasketEntry[j] = fBasketEntryTmp[i];
            fBasketSeek[j] = fBasketSeekTmp[i];
            // Loose basekets aren't stored within another compressed range
            compressedBasketInfo[j] = null;
            logger.trace("Add entry loose " + j + " entry " + fBasketEntry[j]);
            j += 1;
        }

        if (embeddedBaskets > 0) {
            logger.trace(" - Embedded showing up");
            /*
             * In the case of embedded baskets, push forward lastEvent by
             * the number of events in the final loose basket
             */
        } else {
        	logger.trace(" - No embedded baskets");
        }
    	// Put the last entry in the basket past the last one.
        lastEntry = fBasketEntryTmp[looseBaskets];
    	fBasketEntry[j] = lastEntry;

        /*
         * For embedded baskets, we need to (annoyingly) do some math to
         * calculate the fBasketEntry. Reminder: lastEntry is one past
         * the final valid entry in the loose baskets, so when we add the
         * number of entries in the embedded basket, the new lastEntry is
         * also one past the last entry in the embedded basket.
         */
        for (int i = 0; i < embeddedBaskets; i += 1) {
            // We track where the TKey is read in our custom streamer override
            fBasketSeek[j] = fBaskets.getSeek(i);
            Long[] tmpInfo = fBaskets.getCompressedInfo(i);
            Long[] tmpInfo2 = fBaskets.getAddlLongInfo(i);
            Integer[] tmpInfo3 = fBaskets.getAddlIntInfo(i);
            compressedBasketInfo[j] = new CompressedBasketInfo(tmpInfo[0].intValue(),
            						tmpInfo[1].intValue(),
            						tmpInfo[2].intValue(),
            						tmpInfo[3],
            						fBaskets.getBasketBytes(i),
            						tmpInfo2[0],
            						tmpInfo2[1],
            						tmpInfo2[2],
            						tmpInfo3[0],
            						tmpInfo3[1],
            						tmpInfo3[2],
            						tmpInfo3[3],
            						tmpInfo3[4]);
            // fNbytes doesn't show up here for some reason
            // fBasketBytes[j] = (int) basket.getScalar("fNbytes").getVal();

            long entryFromBasket = fBaskets.getfNevBuf(i);
            logger.trace(" - Embedded has " + entryFromBasket + " events");
            /*
             * first set the current basket beginning, then push lastEntry
             * forward past the end of this basket
             */
            fBasketEntry[j] = lastEntry;
            lastEntry += entryFromBasket;
            logger.trace("Add entry embed " + j + " entry " + lastEntry);
            j += 1;
        }

        fMaxBaskets = j;
        /*
         * Finally we add back in the fake basket who is one past the last
         * valid basket.
         */
        fBasketEntry[j] = lastEntry;
        compressedBasketInfo[j] = null;
        logger.trace("     bytes      entry      seek       compressionInfo complete");
        for (int i = 0; i < correctedBasketCount; i += 1) {

            logger.trace(String.format("%04d %10d %10d %10d %s", i, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i], compressedBasketInfo[i]));
        }
        logger.trace("Closing branch " + fName + " fEntries: " + fEntries + " maxBaskets " + fMaxBaskets);
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
        String ret;
        ret = (String) data.getScalar("fName").getVal();
        if (ret.endsWith(".")) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
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

    public synchronized List<TBasket> getBaskets() {
        if (lazyBasketStorage == null) {
            loadBaskets();
        }
        return lazyBasketStorage;
    }

    private void loadBaskets() {
        lazyBasketStorage = new ArrayList<TBasket>();
        TFile backing = tree.getBackingFile();
        for (int i = 0; i < fMaxBaskets; i += 1) {
            if (fBasketSeek[i] == 0) {
                // An empty basket?
                continue;
            }
            try {
                TBasket b;
                if (compressedBasketInfo[i] != null) {
                    CompressedBasketInfo loc = compressedBasketInfo[i];
                    Cursor c = backing.getCursorAt(0);
                    c = c.getPossiblyCompressedSubcursor(loc.getCompressedParentOffset(),
                                                         loc.getCompressedLen(),
                                                         loc.getUncompressedLen(),
                                                         loc.getHeaderLen());
                    c.setOffset(fBasketSeek[i]);
                    b = TBasket.getEmbeddedFromFile(c, loc, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i]);
                } else {
                    Cursor c = backing.getCursorAt(fBasketSeek[i]);
                    b = TBasket.getLooseFromFile(c, fBasketBytes[i], fBasketEntry[i], fBasketSeek[i]);
                }
                lazyBasketStorage.add(b);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
        if (getLeaves().size() == 0) {
            return null;
        } else  if (getLeaves().size() != 1) {
            throw new RuntimeException("Non-split branches are not supported");
        }
        ArrayDescriptor ret = null;
        TLeaf leaf = getLeaves().get(0);
        String title = leaf.getTitle();

        Object className = this.data.getScalar("fClassName");
        if ((ret == null) && (className != null)) {
            switch ((String)this.data.getScalar("fClassName").getVal()) {
                case "vector<bool>":
                case "vector<char>":
                case "vector<unsigned char>":
                case "vector<short>":
                case "vector<unsigned short>":
                case "vector<int>":
                case "vector<unsigned int>":
                case "vector<long>":
                case "vector<unsigned long>":
                case "vector<float>":
                case "vector<double>":
                    /*
                     *  need to treat this as a float array and skip the
                     *  first 10 bytes to skip the vector stuff. See Uproot
                     *  interp/auto.py
                     */
                    return ArrayDescriptor.newVarArray("", 10);
                default:
                    break;
            }
        }

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
        if (leaves.size() == 1) {
            TLeaf leaf = leaves.get(0);
            if (getTitle().length() >= 2) {
                ret = getTypeFromTitle(getTitle());
            }

            Object className = this.data.getScalar("fClassName");
            if ((ret == null) && (className != null)) {
                switch ((String)this.data.getScalar("fClassName").getVal()) {
                    // See Uproot interp/auto.py
                    case "vector<bool>":
                        ret = new SimpleType.ArrayType(SimpleType.Bool);
                        break;
                    case "vector<char>":
                        ret = new SimpleType.ArrayType(SimpleType.Int8);
                        break;
                    case "vector<unsigned char>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt8);
                        break;
                    case "vector<short>":
                        ret = new SimpleType.ArrayType(SimpleType.Int16);
                        break;
                    case "vector<unsigned short>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt16);
                        break;
                    case "vector<int>":
                        ret = new SimpleType.ArrayType(SimpleType.Int32);
                        break;
                    case "vector<unsigned int>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt32);
                        break;
                    case "vector<long>":
                        ret = new SimpleType.ArrayType(SimpleType.Int64);
                        break;
                    case "vector<unsigned long>":
                        ret = new SimpleType.ArrayType(SimpleType.UInt64);
                        break;
                    case "vector<float>":
                        ret = new SimpleType.ArrayType(SimpleType.Float32);
                        break;
                    case "vector<double>":
                        ret = new SimpleType.ArrayType(SimpleType.Float64);
                        break;
                    default:
                        break;
                }
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
        int basketCount = fBasketEntry.length;
        // The array processing code wants a final entry to cap the last true
        // basket from above
        if (fBasketEntry[basketCount - 1] == tree.getEntries()) {
            long []ret = new long[basketCount];
            for (int i = 0; i < basketCount; i += 1) {
                ret[i] = fBasketEntry[i];
            }
            return ret;
        } else {
            long []ret = new long[basketCount + 1];
            for (int i = 0; i < basketCount; i += 1) {
                ret[i] = fBasketEntry[i];
            }
            ret[basketCount] = tree.getEntries();
            return ret;
        }
    }

    public int getBasketCount() {
        return fMaxBaskets;
    }

    public int[] getBasketBytes() {
        return fBasketBytes;
    }

    public long[] getBasketSeek() {
        return fBasketSeek;
    }

    /**
     * Converts a root-style basketEntryOffset into a RangeMap which maps
     * (long) entries to (int)basketIDs. This is a bit more complicated than
     * the regular RangeMap intersection function, because when we decode
     * baskets, we need to process a whole basket at a time, but intersect()
     * will trim the ranges to line up with our input range
     *
     * @param basketEntryOffsets ROOTs value + the cap end to close the last
     * @param entrystart the beginning entry we want
     * @param entrystop one past the last entry we want
     * @return RangeMap of all the baskets containing our range
     */
    public static ImmutableRangeMap<Long, Integer> entryOffsetToRangeMap(long[] basketEntryOffsets, long entrystart, long entrystop) {
        Builder<Long, Integer> basketBuilder = new ImmutableRangeMap.Builder<Long, Integer>();
        for (int i = 0; i < basketEntryOffsets.length - 1; i += 1) {
            basketBuilder = basketBuilder.put(Range.closed(basketEntryOffsets[i], basketEntryOffsets[i + 1] - 1), i);
        }
        ImmutableRangeMap<Long, Integer> tmpMap = basketBuilder.build();
        Entry<Range<Long>, Integer> low = tmpMap.getEntry(entrystart);
        Entry<Range<Long>, Integer> high = tmpMap.getEntry(entrystop - 1);
        Range<Long> wideRange = Range.closed(low.getKey().lowerEndpoint(), high.getKey().upperEndpoint());
        return tmpMap.subRangeMap(wideRange);
    }

    /**
     * @return the compressedBasketInfo
     */
    public CompressedBasketInfo[] getCompressedBasketInfo() {
        return compressedBasketInfo;
    }

	public int getMaxBaskets() {
		return fMaxBaskets;
	}

	public int getBasketBytes(int i) {
		return fBasketBytes[i];
	}

	public long getBasketEntry(int i) {
		return fBasketEntry[i];
	}

	public long getBasketSeek(int i) {
		return fBasketSeek[i];
	}
}
