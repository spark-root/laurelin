package edu.vanderbilt.accre.laurelin.root_proxy;

public class SimpleType {
    public static class ScalarType extends SimpleType { }
    public static class ArrayType extends SimpleType {
        private SimpleType childType;
        public ArrayType(SimpleType childType) {
            this.childType = childType;
        }
        public SimpleType getChildType() {
            return childType;
        }
    }
    public static class NumericType extends ScalarType { }
    public static class BoolType extends NumericType { }
    public static class Int8Type extends NumericType { }
    public static class Int16Type extends NumericType { }
    public static class Int32Type extends NumericType { }
    public static class Int64Type extends NumericType { }
    public static class UInt8Type extends NumericType { }
    public static class UInt16Type extends NumericType { }
    public static class UInt32Type extends NumericType { }
    public static class UInt64Type extends NumericType { }
    public static class Float32Type extends NumericType { }
    public static class Float64Type extends NumericType { }
    public static class PointerType extends ScalarType {}

    public static final PointerType Pointer = new PointerType();
    public static final BoolType Bool = new BoolType();
    public static final Int8Type Int8 = new Int8Type();
    public static final Int16Type Int16 = new Int16Type();
    public static final Int32Type Int32 = new Int32Type();
    public static final Int64Type Int64 = new Int64Type();
    public static final UInt8Type UInt8 = new UInt8Type();
    public static final UInt16Type UInt16 = new UInt16Type();
    public static final UInt32Type UInt32 = new UInt32Type();
    public static final UInt64Type UInt64 = new UInt64Type();
    public static final Float32Type Float32 = new Float32Type();
    public static final Float64Type Float64 = new Float64Type();
}