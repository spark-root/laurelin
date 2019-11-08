package edu.vanderbilt.accre.spark_ttree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch;
import edu.vanderbilt.accre.laurelin.spark_ttree.SlimTBranch.SlimTBasket;

public class SlimTBranchTest {

    @Test
    public void test() throws ClassNotFoundException, IOException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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

        SlimTBranch middle2 = full.copyAndTrim(10, 25);
        Method method = middle2.getClass().getDeclaredMethod("writeReplace");
        method.setAccessible(true);
        SlimTBranch.SerializeStorage r = (SlimTBranch.SerializeStorage) method.invoke(middle2);
        middle2 = roundTrip(middle2);
        assertNotNull(r.getRangeToBasketID()[0]);
        assertNotEquals(null, r.getRangeToBasketID()[0]);
        long[] middleOffset2 = middle2.getBasketEntryOffsets();
        assertEquals(10, middleOffset2[1]);
        assertEquals(22, middleOffset2[2]);
        assertEquals(2, middle2.getStoredBasketCount());

        SlimTBranch end = roundTrip(full.copyAndTrim(20, 41));
        long[] endOffset = end.getBasketEntryOffsets();
        assertEquals(10, endOffset[1]);
        assertEquals(22, endOffset[2]);
        assertEquals(30, endOffset[3]);
        assertEquals(41, endOffset[4]);
        assertEquals(3, end.getStoredBasketCount());
    }

    public SlimTBranch roundTrip(SlimTBranch val) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteArrayInputStream bis;
        ObjectOutput out = null;
        byte[] yourBytes = null;
        out = new ObjectOutputStream(bos);
        out.writeObject(val);
        out.flush();
        yourBytes = bos.toByteArray();
        /*
         *  This patch produces 348555 bytes serialized, ensure it doesn't
         *  grow accidentally
         */
        assertTrue("Partition size too large", yourBytes.length < 349000);

        System.out.println("Got length: " + yourBytes.length);
        bis = new ByteArrayInputStream(yourBytes);
        ObjectInput in = new ObjectInputStream(bis);
        return (SlimTBranch) in.readObject();
    }
}
