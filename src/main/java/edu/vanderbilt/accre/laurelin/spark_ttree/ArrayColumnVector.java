package edu.vanderbilt.accre.laurelin.spark_ttree;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.PrimitiveArray;

public class ArrayColumnVector extends ColumnVector {
    private Array array;

    public ArrayColumnVector(DataType type, Array array) {
        super(type);
        this.array = array;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean hasNull() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int numNulls() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isNullAt(int rowId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return ((PrimitiveArray.Bool)array).toBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return ((PrimitiveArray.Int1)array).toByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return ((PrimitiveArray.Int2)array).toShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return ((PrimitiveArray.Int4)array).toInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return ((PrimitiveArray.Int8)array).toLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return ((PrimitiveArray.Float4)array).toFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return ((PrimitiveArray.Float8)array).toDouble(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getBinary(int rowId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected ColumnVector getChild(int ordinal) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * Return
     */

    @Override
    public boolean[] getBooleans(int rowId, int count) {
        return (boolean[])array.toArray();
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        return (byte[])array.toArray();
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        return (short[])array.toArray();
    }

    @Override
    public int[] getInts(int rowId, int count) {
        return (int[])array.toArray();
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        return (long[])array.toArray();
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        return (float[])array.toArray();
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        // TODO Auto-generated method stub
        return super.getDoubles(rowId, count);
    }
}
