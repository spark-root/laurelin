package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsJagged;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;
import edu.vanderbilt.accre.laurelin.root_proxy.TFile;
import edu.vanderbilt.accre.laurelin.root_proxy.TLeaf;
import edu.vanderbilt.accre.laurelin.root_proxy.TTree;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.JaggedArrayPrep;

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
        String testPath = "nano_19.root";
        String testTree = "Events";
        File f = new File(testPath);
        TFile currFile = TFile.getFromFile(testPath);
        return new TTree(currFile.getProxy(testTree), currFile);
    }

    @Test
    public void testNano19() throws IOException {
        TTree currTree = getTestTree();
        List<TBranch> branches = currTree.getBranches("Jet_pt");
        TBranch branch = branches.get(0);
        List<TBasket> baskets = branch.getBaskets();
        Cache branchCache = new Cache();
        SlimTBranch slimBranch = SlimTBranch.getFromTBranch(branch);
        ArrayBuilder.GetBasket getbasket = slimBranch.getArrayBranchCallback(branchCache);
        long []basketEntryOffsets = slimBranch.getBasketEntryOffsets();
        Interpretation interp = new AsJagged(new AsDtype(AsDtype.Dtype.FLOAT4));
        ArrayBuilder builder = new ArrayBuilder(getbasket, interp, basketEntryOffsets, null, 0, 50069);
        JaggedArrayPrep testarray = (JaggedArrayPrep)builder.getArray(0, 10);
        System.out.println(Arrays.toString((int[])(testarray.counts().toArray(true))));
        System.out.println(Arrays.toString((float[])(testarray.content().toArray(true))));
    }

}
