package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch.SlimTBasket;

public class SlimTBranchTest {

    @Test
    public void test() {
        long[] basketEntryOffsetFull = new long[] {0, 10, 22, 30, 41};
        long[] basketEntrySeekFull = new long[] {0, 1, 2, 3};

        SlimTBranch full = new SlimTBranch("none", basketEntryOffsetFull, null);
        for (int i = 0; i < basketEntrySeekFull.length; i += 1) {
            full.addBasket(i, SlimTBasket.makeLazyBasket(basketEntrySeekFull[i]));
        }
        assertArrayEquals(basketEntryOffsetFull, full.getBasketEntryOffsets());
        assertEquals(4, full.getStoredBasketCount());

        SlimTBranch beginning = full.copyAndTrim(0, 10);
        long[] beginOffset = beginning.getBasketEntryOffsets();
        assertEquals(0, beginOffset[0]);
        assertEquals(1, beginning.getStoredBasketCount());

        SlimTBranch middle = full.copyAndTrim(10, 25);
        long[] middleOffset = middle.getBasketEntryOffsets();
        assertEquals(10, middleOffset[1]);
        assertEquals(22, middleOffset[2]);
        assertEquals(2, middle.getStoredBasketCount());

        SlimTBranch end = full.copyAndTrim(20, 41);
        long[] endOffset = end.getBasketEntryOffsets();
        assertEquals(10, endOffset[1]);
        assertEquals(22, endOffset[2]);
        assertEquals(30, endOffset[3]);
        assertEquals(41, endOffset[4]);
        assertEquals(3, end.getStoredBasketCount());
    }
}
