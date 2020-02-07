package edu.vanderbilt.accre.laurelin;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;

public class InterpretationsTest {
    @Test
    public void asdtype() {
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.INT4);

        Array destination = asdtype.destination(10, 10);
        Assert.assertEquals(Arrays.toString((int[])destination.toArray()), Arrays.toString(new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));

        Array one = asdtype.fromroot(new PrimitiveArray.Int4(new int[]{0,1,2,3,4}, true).rawarray(), null, 0, 5);
        Array two = asdtype.fromroot(new PrimitiveArray.Int4(new int[]{5,6,7,8,9}, true).rawarray(), null, 0, 5);
        Assert.assertEquals(Arrays.toString((int[])one.toArray()), Arrays.toString(new int[]{0,1,2,3,4}));
        Assert.assertEquals(Arrays.toString((int[])two.toArray()), Arrays.toString(new int[]{5,6,7,8,9}));

        asdtype.fill(one, destination, 0, 5, 0, 5);
        Assert.assertEquals(Arrays.toString((int[])destination.toArray()), Arrays.toString(new int[]{0, 1,2,3,4, 0, 0, 0, 0, 0}));

        asdtype.fill(two, destination, 5, 10, 5, 10);
        Assert.assertEquals(Arrays.toString((int[])destination.toArray()), Arrays.toString(new int[]{0, 1,2,3,4,5,6,7,8,9}));

        Array clipped = asdtype.clip(destination, 1, 9);
        Assert.assertEquals(Arrays.toString((int[])clipped.toArray()), Arrays.toString(new int[]{1,2,3,4,5,6,7,8}));

        Array finalized = asdtype.finalize(clipped);
        Assert.assertEquals(Arrays.toString((int[])finalized.toArray()), Arrays.toString(new int[]{1,2,3,4,5,6,7,8}));
    }
}
