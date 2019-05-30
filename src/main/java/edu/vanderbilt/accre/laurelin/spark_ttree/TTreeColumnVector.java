package edu.vanderbilt.accre.laurelin.spark_ttree;

import java.util.List;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import edu.vanderbilt.accre.laurelin.Cache;
import edu.vanderbilt.accre.laurelin.array.Array;
import edu.vanderbilt.accre.laurelin.array.ArrayBuilder;
import edu.vanderbilt.accre.laurelin.interpretation.Interpretation;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.root_proxy.TBasket;
import edu.vanderbilt.accre.laurelin.root_proxy.TBranch;

public class TTreeColumnVector extends ColumnVector {
    private Cache basketCache;
    private TBranch branch;
    private long [] basketEntryOffsets;
    private ArrayBuilder.GetBasket getbasket;
    private ArrayBuilder builder;

    public TTreeColumnVector(DataType type, TBranch branch, Cache basketCache, long entrystart, long entrystop) {
        super(type);
        this.branch = branch;

        this.basketEntryOffsets = branch.getBasketEntryOffsets();
        this.getbasket = branch.getArrayBranchCallback(basketCache);
        this.basketCache = basketCache;

        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);

        TBranch.ArrayDescriptor desc = branch.getArrayDescriptor();
        if (desc == null) {
            AsDtype interpretation = new AsDtype(AsDtype.Dtype.FLOAT4);   // FIXME
            this.builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, executor, entrystart, entrystop);
        }
        else if (desc.isFixed()) {
            AsDtype interpretation = new AsDtype(AsDtype.Dtype.FLOAT4, Arrays.asList(desc.getFixedLength()));   // FIXME
            this.builder = new ArrayBuilder(getbasket, interpretation, basketEntryOffsets, executor, entrystart, entrystop);
        } else {
            //
        }
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
        return getFloats(rowId, 1)[0];
    }

    @Override
    public double getDouble(int rowId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ColumnarArray getArray(int rowId) {
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
        // TODO Auto-generated method stub
        return super.getBooleans(rowId, count);
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        // TODO Auto-generated method stub
        return super.getBytes(rowId, count);
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        // TODO Auto-generated method stub
        return super.getShorts(rowId, count);
    }

    @Override
    public int[] getInts(int rowId, int count) {
        // TODO Auto-generated method stub
        return super.getInts(rowId, count);
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        // TODO Auto-generated method stub
        return super.getLongs(rowId, count);
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        return (float[])(builder.getArray(rowId, count).toArray());
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        return (double[])(builder.getArray(rowId, count).toArray());
    }
}
