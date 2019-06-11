package edu.vanderbilt.accre.laurelin.root_proxy;

public class TLeaf extends TBranch {

    public TLeaf(Proxy data, TTree tree, TBranch parent) {
        super(data, tree, parent);
    }

    @Override
    public boolean typeUnhandled() {
        if (getClassName().equals("TLeafC")) {
            return true;
        } else if (getClassName().equals("TLeafElement") && ((getType() == 65) || (getType() == 300))) {
            // kTString
            return true;
        }
        return false;
    }

    private SimpleType getTypeFromSimpleLeafType(String type) {
        SimpleType ret = null;
        // fLen needs to be zero
        switch (data.className) {
        case ("TLeafO"):
            ret = SimpleType.Bool;
        break;
        case ("TLeafB"):
            ret = SimpleType.Int8;
        break;
        case ("TLeafS"):
            ret = SimpleType.Int16;
        break;
        case ("TLeafI"):
            ret = SimpleType.Int32;
        break;
        case ("TLeafL"):
            ret = SimpleType.Int64;
        break;
        case ("TLeafF"):
            ret = SimpleType.Float32;
        break;
        case ("TLeafD"):
            ret = SimpleType.Float64;
        break;
        }
        return ret;
    }

    public SimpleType getLeafType() {
        /*
         * An attempt to decode all the different ROOT type systems to
         * an easy descriptor
         */
        SimpleType ret = null;
        if (getTitle().length() >= 2) {
            ret = getTypeFromTitle(getTitle());
        }

        if (ret == null) {
            ret = getTypeFromSimpleLeafType(data.className);
        }

        if (data.className.equals("TLeafElement")) {
            int fType = getNormedType();
            if ((ret == null) && (fType > Constants.kOffsetP) && (fType < Constants.kObject)) {
                // I hate this so much
                // Dig through the TLeafElement to get at the information
                fType = (int) data.getProxy("fLeafCount").getScalar("fType").getVal();
            }
            switch (fType) {
            case 2:
                // kShort
                ret = SimpleType.Int16;
                break;
            case 3:
                // kInt
                ret = SimpleType.Int32;
                break;
            case 4:
                // kLong
                ret = SimpleType.Int64;
                break;
            case 5:
                // kFloat
                ret = SimpleType.Float32;
                break;
            case 6:
                // kCounter
                ret = SimpleType.UInt32;
                break;
            case 8:
                // kDouble
                ret = SimpleType.Float64;
                break;
            case 12:
                // kUShort
                ret = SimpleType.UInt16;
                break;
            case 13:
                // kUInt
                ret = SimpleType.UInt32;
                break;
            case 14:
                // kULong
                ret = SimpleType.UInt64;
                break;
            }

        }

        if (ret == null) {
            data.dump();
            throw new RuntimeException("Unknown TLeaf type. name: " + getName() + " class: " + data.className + " type " + getType() + "\n" + data.dumpStr());
        }

        // Do I later want to separate fixed and not-fixed arrays?
        if ( ((int) data.getScalar("fLen").getVal() > 1) ||
                (data.getProxy("fLeafCount").getDataSize() != 0)){
            ret = new SimpleType.ArrayType(ret);
        }
        return ret;
    }
}
