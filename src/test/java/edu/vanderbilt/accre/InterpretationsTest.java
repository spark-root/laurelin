package edu.vanderbilt.accre;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class InterpretationsTest {
    @Test
    public void mycode() {
        System.out.println("This is where I'll put most of my initial work.");
        assertTrue(true);
    }
}


// scala> import edu.vanderbilt.accre.interpretation.AsDtype
// import edu.vanderbilt.accre.interpretation.AsDtype

// scala> import edu.vanderbilt.accre.array.PrimitiveArrayFloat8
// import edu.vanderbilt.accre.array.PrimitiveArrayFloat8

// scala> val d = new AsDtype(AsDtype.Dtype.FLOAT8)
// d: edu.vanderbilt.accre.interpretation.AsDtype = edu.vanderbilt.accre.interpretation.AsDtype@4a70d302

// scala> val dest = d.destination(10, 10)
// dest: edu.vanderbilt.accre.array.Array[_] = edu.vanderbilt.accre.array.PrimitiveArrayFloat8@7eaa2bc6

// scala> dest.toArray(true)
// res0: Object = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

// scala> d.fill(d.fromroot(new PrimitiveArrayFloat8(Array(0, 1.1, 2.2, 3.3, 4.4), true).rawarray(), null, 0, 5), dest, 5, 10, -1, -1)

// scala> dest.toArray(true)
// res2: Object = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.1, 2.2, 3.3, 4.4)

// scala> d.fill(d.fromroot(new PrimitiveArrayFloat8(Array(0, 1.1, 2.2, 3.3, 4.4), true).rawarray(), null, 0, 5), dest, 0, 5, -1, -1)

// scala> dest.toArray(true)
// res4: Object = Array(0.0, 1.1, 2.2, 3.3, 4.4, 0.0, 1.1, 2.2, 3.3, 4.4)

