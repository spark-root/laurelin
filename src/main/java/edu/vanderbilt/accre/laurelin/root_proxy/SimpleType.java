package edu.vanderbilt.accre.laurelin.root_proxy;

import java.util.HashMap;
import java.util.Map;

import edu.vanderbilt.accre.laurelin.interpretation.AsDtype;
import edu.vanderbilt.accre.laurelin.interpretation.AsDtype.Dtype;

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
        @Override
        public SimpleType getBaseType() {
            return childType.getBaseType();
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

    public SimpleType getBaseType() {
        return this;
    }
    /*
     * We need to serialize the root type names to strings to pass it to
     * executors
     */

    @Override
    public String toString() {
        return typeToStringMap.get(this.getClass());
    }

    public static SimpleType fromString(String str) {
        return stringToTypeMap.get(str);
    }

    public static Dtype dtypeFromString(String str) {
        return stringToDtypeMap.get(str);
    }

    private static Map<Class<? extends SimpleType>, String> typeToStringMap;
    private static Map<String, SimpleType> stringToTypeMap;
    private static Map<String, Dtype> stringToDtypeMap;

    static {
        typeToStringMap = new HashMap<Class<? extends SimpleType>, String>();
        typeToStringMap.put(BoolType.class, "bool");
        typeToStringMap.put(Int8Type.class, "char");
        typeToStringMap.put(Int16Type.class, "short");
        typeToStringMap.put(Int32Type.class, "int");
        typeToStringMap.put(Int64Type.class, "long");
        typeToStringMap.put(UInt8Type.class, "uchar");
        typeToStringMap.put(UInt16Type.class, "ushort");
        typeToStringMap.put(UInt32Type.class, "uint");
        typeToStringMap.put(UInt64Type.class, "ulong");
        typeToStringMap.put(Float32Type.class, "float");
        typeToStringMap.put(Float64Type.class, "double");

        stringToTypeMap = new HashMap<String, SimpleType>();
        stringToTypeMap.put("bool", SimpleType.Bool);
        stringToTypeMap.put("char", SimpleType.Int8);
        stringToTypeMap.put("short", SimpleType.Int16);
        stringToTypeMap.put("int", SimpleType.Int32);
        stringToTypeMap.put("long", SimpleType.Int64);
        stringToTypeMap.put("uchar", SimpleType.UInt8);
        stringToTypeMap.put("ushort", SimpleType.UInt16);
        stringToTypeMap.put("uint", SimpleType.UInt32);
        stringToTypeMap.put("ulong", SimpleType.UInt64);
        stringToTypeMap.put("float", SimpleType.Float32);
        stringToTypeMap.put("double", SimpleType.Float64);

        stringToDtypeMap = new HashMap<String, Dtype>();
        stringToDtypeMap.put("bool", AsDtype.Dtype.BOOL);
        stringToDtypeMap.put("char", AsDtype.Dtype.INT1);
        stringToDtypeMap.put("short", AsDtype.Dtype.INT2);
        stringToDtypeMap.put("int", AsDtype.Dtype.INT4);
        stringToDtypeMap.put("long", AsDtype.Dtype.INT8);
        stringToDtypeMap.put("uchar", AsDtype.Dtype.UINT1);
        stringToDtypeMap.put("ushort", AsDtype.Dtype.UINT2);
        stringToDtypeMap.put("uint", AsDtype.Dtype.UINT4);
        stringToDtypeMap.put("ulong", AsDtype.Dtype.UINT8);
        stringToDtypeMap.put("float", AsDtype.Dtype.FLOAT4);
        stringToDtypeMap.put("double", AsDtype.Dtype.FLOAT8);
    }
}