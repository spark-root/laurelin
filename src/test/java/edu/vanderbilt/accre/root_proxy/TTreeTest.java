package edu.vanderbilt.accre.root_proxy;

import static edu.vanderbilt.accre.Helpers.getBigTestDataIfExists;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.cache.BasketCache;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsJagged;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TLeaf;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranchInterface;

public class TTreeTest {
    private TTree getTestTree() throws IOException {
        String testPath = "testdata/uproot-small-flat-tree.root";
        String testTree = "tree";
        TFile currFile = TFile.getFromFile(testPath);
        return new TTree(currFile.getProxy(testTree), currFile);
    }

    private TTree getBigTestTree() throws IOException {
        String testPath = getBigTestDataIfExists("testdata/A2C66680-E3AA-E811-A854-1CC1DE192766.root");
        String testTree = "Events";
        TFile currFile = TFile.getFromFile(testPath);
        return new TTree(currFile.getProxy(testTree), currFile);
    }

    @Test
    public void testEntryCount() throws IOException {
        TTree currTree = getTestTree();
        assertEquals(100, currTree.getEntries());
    }

    @Test
    public void testGetBranches_all() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches();
        assertEquals(19, branches.size());
    }

    @Test
    public void testGetBranchBasket_float64() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches();
        assertEquals(19, branches.size());
        for (TBranch branch: branches) {
            System.out.println(branch.getName() + " - " + branch.getTitle());
            for (TLeaf leaf: branch.getLeaves()) {
                System.out.println("  " + leaf.getName() + " - " + leaf.getTitle());
            }
        }
        branches = currTree.getBranches("Float64");
        TBranch branch = branches.get(0);
        List<TBasket> baskets = branch.getBaskets();
        assertEquals(1, baskets.size());
        TBasket basket = baskets.get(0);
        ByteBuffer buf = basket.getPayload();
        // compressed basket size
        assertEquals(297, basket.getBasketBytes());
        // uncompressed basket size - 8 bytes/entry * 100 entries
        assertEquals(800, buf.limit());

        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);

        BasketCache branchCache = new BasketCache();
        SlimTBranchInterface slimBranch = SlimTBranch.getFromTBranch(branch);
        ArrayBuilder.GetBasket getbasket = slimBranch.getArrayBranchCallback(branchCache, null);
        long []basketEntryOffsets = slimBranch.getBasketEntryOffsets();
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.FLOAT8);
        ArrayBuilder builder = new ArrayBuilder(getbasket, asdtype, basketEntryOffsets, executor, 1, 9);
        double [] testarray = (double[])builder.getArray(0, 8).toArray();
    }

    @Test
    public void testGetBranchBasket_slicefloat32() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches();
        assertEquals(19, branches.size());
        branches = currTree.getBranches("SliceFloat32");
        TBranch branch = branches.get(0);
        SlimTBranchInterface slimBranch = SlimTBranch.getFromTBranch(branch);
        List<TBasket> baskets = branch.getBaskets();
        assertEquals(1, baskets.size());
        TBasket basket = baskets.get(0);
        ByteBuffer buf = basket.getPayload();

        BasketCache branchCache = new BasketCache();
        ArrayBuilder.GetBasket getbasket = slimBranch.getArrayBranchCallback(branchCache, null);
        long [] basketEntryOffsets = branch.getBasketEntryOffsets(); //{ 0, 100 };
        Interpretation interpretation = new AsJagged(new AsDtype(AsDtype.Dtype.FLOAT4));
        ArrayBuilder builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, null, 0, 100);
        builder.getArray(0, 100);
    }

    @Test
    public void testGetBranchBasket_bigfloat32() throws IOException {
        TTree currTree = getBigTestTree();
        List<TBranch> branches = currTree.getBranches();
        assertEquals(866, branches.size());
        branches = currTree.getBranches("CaloMET_phi");
        TBranch branch = branches.get(0);
        List<TBasket> baskets = branch.getBaskets();
        assertEquals(13, baskets.size());
        TBasket basket = baskets.get(0);
        ByteBuffer buf = basket.getPayload();
        // compressed basket size
        assertEquals(2309, basket.getBasketBytes());
        // uncompressed basket size - 4 bytes/entry * 1000 entries
        assertEquals(4000, buf.limit());

        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);

        BasketCache branchCache = new BasketCache();
        SlimTBranchInterface slimBranch = SlimTBranch.getFromTBranch(branch);
        ArrayBuilder.GetBasket getbasket = slimBranch.getArrayBranchCallback(branchCache, null);
        long [] basketEntryOffsets = slimBranch.getBasketEntryOffsets(); //{ 0, 100 };
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.FLOAT4);
        ArrayBuilder builder = new ArrayBuilder(getbasket, asdtype, basketEntryOffsets, executor, 1, 9);
    }

    @Test
    public void testGetBranchBasket() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches("Float64");
        assertEquals(1, branches.size());
        TBranch branch = branches.get(0);
        assertEquals("Float64", branch.getName());
        assertEquals(1, branch.getLeaves().size());
        TLeaf leaf = branch.getLeaves().get(0);
    }

    @Test
    public void testGetBranchList_success_one() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches("N");
        assertEquals(1, branches.size());
    }

    @Test
    public void testGetBranchList_success_two() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches("N", "Int64");
        assertEquals(2, branches.size());
    }

    @Test
    public void testGetBranchList_success_two_array() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches(new String[] {"N", "Int64"});
        assertEquals(2, branches.size());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBranchList_fail_noexist() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches("does not exist");
    }

    @Test(expected = RuntimeException.class)
    public void testGetBranchList_fail_skipped() throws IOException {
        TTree currTree = getTestTree();
        // We don't parse string branches
        List<TBranch> branches = currTree.getBranches("Str");
    }
}
