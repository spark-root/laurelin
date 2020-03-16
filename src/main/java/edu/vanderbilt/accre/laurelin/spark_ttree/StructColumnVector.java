package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class StructColumnVector extends ColumnVector {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();
    LinkedList<ColumnVector> fields;

    public StructColumnVector(DataType type, LinkedList<ColumnVector> nestedVecs) {
        super(type);
        logger.trace("new struct vec of type: " + type);
        fields = nestedVecs;
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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public byte getByte(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public short getShort(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getInt(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLong(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public float getFloat(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double getDouble(int rowId) {
        // TODO Auto-generated method stub
        return 0;
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
    public ColumnVector getChild(int ordinal) {
        return fields.get(ordinal);
    }

}
