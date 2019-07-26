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

}
