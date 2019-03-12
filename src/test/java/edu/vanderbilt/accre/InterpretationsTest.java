package edu.vanderbilt.accre;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import edu.vanderbilt.accre.interpretation.AsDtype;
import edu.vanderbilt.accre.array.Array;
import edu.vanderbilt.accre.array.PrimitiveArray;

public class InterpretationsTest {
    @Test
    public void asdtype() {
        AsDtype asdtype = new AsDtype(AsDtype.Dtype.FLOAT8);

        Array destination = asdtype.destination(10, 10);
        Assert.assertEquals(Arrays.toString((double[])destination.toArray()), Arrays.toString(new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}));

        Array one = asdtype.fromroot(new PrimitiveArray.Float8(new double[]{0.0, 1.1, 2.2, 3.3, 4.4}, true).rawarray(), null, 0, 5);
        Array two = asdtype.fromroot(new PrimitiveArray.Float8(new double[]{5.5, 6.6, 7.7, 8.8, 9.9}, true).rawarray(), null, 0, 5);
        Assert.assertEquals(Arrays.toString((double[])one.toArray()), Arrays.toString(new double[]{0.0, 1.1, 2.2, 3.3, 4.4}));
        Assert.assertEquals(Arrays.toString((double[])two.toArray()), Arrays.toString(new double[]{5.5, 6.6, 7.7, 8.8, 9.9}));

        asdtype.fill(one, destination, 0, 5, 0, 5);
        Assert.assertEquals(Arrays.toString((double[])destination.toArray()), Arrays.toString(new double[]{0.0, 1.1, 2.2, 3.3, 4.4, 0.0, 0.0, 0.0, 0.0, 0.0}));

        asdtype.fill(two, destination, 5, 10, 5, 10);
        Assert.assertEquals(Arrays.toString((double[])destination.toArray()), Arrays.toString(new double[]{0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9}));

        Array clipped = asdtype.clip(destination, 1, 9, 1, 9);
        Assert.assertEquals(Arrays.toString((double[])clipped.toArray()), Arrays.toString(new double[]{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8}));

        Array finalized = asdtype.finalize(clipped);
        Assert.assertEquals(Arrays.toString((double[])finalized.toArray()), Arrays.toString(new double[]{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8}));
    }
}
