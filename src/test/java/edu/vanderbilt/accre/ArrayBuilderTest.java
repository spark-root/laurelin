package edu.vanderbilt.accre;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;

public class ArrayBuilderTest {
    @Test
    public void asdtype() {
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.FLOAT8);

        ArrayBuilder.GetBasket getbasket = new ArrayBuilder.GetBasket() {
                public ArrayBuilder.BasketKey basketkey(int basketid) {
                    return new ArrayBuilder.BasketKey(0, 8*5, 8*5);
                }
                public RawArray dataWithoutKey(int basketid) {
                    return new PrimitiveArray.Float8(new double[]{0.0, 1.1, 2.2, 3.3, 4.4}, true).rawarray();
                }
            };

        long[] basketEntryOffsets = new long[]{0, 5, 10};

        ArrayBuilder builder = new ArrayBuilder(getbasket, asdtype, basketEntryOffsets);

        Assert.assertEquals(Arrays.toString((double[])builder.build(0, 10).toArray()), Arrays.toString(new double[]{0.0, 1.1, 2.2, 3.3, 4.4, 0.0, 1.1, 2.2, 3.3, 4.4}));

        Assert.assertEquals(Arrays.toString((double[])builder.build(1, 9).toArray()), Arrays.toString(new double[]{1.1, 2.2, 3.3, 4.4, 0.0, 1.1, 2.2, 3.3}));
    }
}
