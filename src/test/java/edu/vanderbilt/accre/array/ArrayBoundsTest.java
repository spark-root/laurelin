package edu.vanderbilt.accre.array;

import static edu.vanderbilt.accre.Helpers.getBigTestDataIfExists;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.JaggedArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.cache.Cache;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsJagged;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranchInterface;

public class ArrayBoundsTest {

    @Test
    public void testNanoBaskets() {
        long[] basketEntryOffsets = {0, 1000, 4266, 8532};
        class DummyBranchCallback implements ArrayBuilder.GetBasket {
            @Override
            public ArrayBuilder.BasketKey basketkey(int basketid) {
                int keylen = 0;
                int last = (int) (2 * (basketEntryOffsets[basketid + 1] - basketEntryOffsets[basketid]));
                int objlen = last;
                return new ArrayBuilder.BasketKey(keylen, last, objlen);
            }

            @Override
            public RawArray dataWithoutKey(int basketid) {
                return new RawArray(ByteBuffer.allocate((int) (2 * (basketEntryOffsets[basketid + 1] - basketEntryOffsets[basketid]))));
            }
        }

        Interpretation interpretation = new AsDtype(AsDtype.Dtype.UINT2);
        ArrayBuilder builder0 = new ArrayBuilder(new DummyBranchCallback(), interpretation, basketEntryOffsets, null, 0, 1000);
        ArrayBuilder builder1 = new ArrayBuilder(new DummyBranchCallback(), interpretation, basketEntryOffsets, null, 1000, 4266);
    }

    private TTree getTestTree() throws IOException {
        String testPath = getBigTestDataIfExists("testdata/nano_19.root");
        String testTree = "Events";
        File f = new File(testPath);
        assumeTrue(f.isFile());
        TFile currFile = TFile.getFromFile(testPath);
        return new TTree(currFile.getProxy(testTree), currFile);
    }

    private void testNano19Impl(String branchName, AsDtype.Dtype type) throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches(branchName);
        TBranch branch = branches.get(0);
        List<TBasket> baskets = branch.getBaskets();
        Cache branchCache = new Cache();
        SlimTBranchInterface slimBranch = SlimTBranch.getFromTBranch(branch);
        ArrayBuilder.GetBasket getbasket = slimBranch.getArrayBranchCallback(branchCache, null);
        long []basketEntryOffsets = slimBranch.getBasketEntryOffsets();
        Interpretation interp = new AsJagged(new AsDtype(type));
        ArrayBuilder builder = new ArrayBuilder(getbasket, interp, basketEntryOffsets, null, 0, 50069);
        JaggedArray testarray = (JaggedArray)builder.getArray(0, 10);
        System.out.println(Arrays.toString((int[])(testarray.counts().toArray(true))));
        System.out.println(Arrays.toString((float[])(testarray.content().toArray(true))));
    }

    @Test
    public void testNano19_jetPt() throws IOException {
        testNano19Impl("Jet_pt", AsDtype.Dtype.FLOAT4);
    }

}
