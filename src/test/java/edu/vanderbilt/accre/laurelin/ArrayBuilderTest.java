package edu.vanderbilt.accre.laurelin;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Assert;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;

public class ArrayBuilderTest {
    @Test
    public void asdtype() {
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.INT4);

        ArrayBuilder.GetBasket getbasket = new ArrayBuilder.GetBasket() {
                @Override
                public ArrayBuilder.BasketKey basketkey(int basketid) {
                    return new ArrayBuilder.BasketKey(0, 4 * 5, 4 * 5);
                }

                @Override
                public RawArray dataWithoutKey(int basketid) {
                    return new PrimitiveArray.Int4(new int[]{0,1,2,3,4}, true).rawarray();
                }
            };

        long[] basketEntryOffsets = new long[]{0, 5, 10};

        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);

        Assert.assertEquals(Arrays.toString((int[])(new ArrayBuilder(getbasket, asdtype, basketEntryOffsets, executor, 0, 10)).getArray(0, 10).toArray()), Arrays.toString(new int[]{0,1,2,3,4,0,1,2,3,4}));

        Assert.assertEquals(Arrays.toString((int[])(new ArrayBuilder(getbasket, asdtype, basketEntryOffsets, executor, 1, 9)).getArray(0, 8).toArray()), Arrays.toString(new int[]{1,2,3,4,0,1,2,3}));
    }
}
