package edu.vanderbilt.accre.array;

import java.nio.ByteBuffer;

import org.junit.Test;

import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.array.RawArray;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;

public class ArrayBoundsTest {

    @Test
    public void testNanoBaskets() {
        long[] basketEntryOffsets = {0, 1000, 4266, 8532};
        class DummyBranchCallback implements ArrayBuilder.GetBasket {
            @Override
            public ArrayBuilder.BasketKey basketkey(int basketid) {
                return new ArrayBuilder.BasketKey(0, 2 * (int)basketEntryOffsets[basketid + 1], (int) (2 * (basketEntryOffsets[basketid + 1] - basketEntryOffsets[basketid])));
            }

            @Override
            public RawArray dataWithoutKey(int basketid) {
                return new RawArray(ByteBuffer.allocate(1000090));
            }
        }

        Interpretation interpretation = new AsDtype(AsDtype.Dtype.UINT2);
        ArrayBuilder builder0 = new ArrayBuilder(new DummyBranchCallback(), interpretation, basketEntryOffsets, null, 0, 1000);
        ArrayBuilder builder1 = new ArrayBuilder(new DummyBranchCallback(), interpretation, basketEntryOffsets, null, 1000, 4266);
    }

}
