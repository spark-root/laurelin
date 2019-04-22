package edu.vanderbilt.accre.laurelin.root_proxy;

public class TLeaf extends TBranch {

	public TLeaf(Proxy data, TTree tree, TBranch parent) {
		super(data, tree, parent);
	}

	@Override
	public boolean typeUnhandled() {
		if (getClassName().equals("TLeafC")) {
			System.out.println("Dropping " + getName());
			return true;
		}
		return false;
	}

	@Override
	public SimpleType getSimpleType() {
		/*
		 * An attempt to decode all the different ROOT type systems to 
		 * an easy descriptor 
		 */
		SimpleType ret = null;
		
		// fLen needs to be zero
		switch (data.className) {
		case ("TLeafO"):
			ret = (SimpleType) SimpleType.Bool;
			break;
		case ("TLeafB"):
			ret = (SimpleType) SimpleType.Int8;
			break;
		case ("TLeafS"):
			ret = (SimpleType) SimpleType.Int16;
			break;
		case ("TLeafI"):
			ret = (SimpleType) SimpleType.Int32;
			break;
		case ("TLeafL"):
			ret = (SimpleType) SimpleType.Int64;
			break;
		case ("TLeafF"):
			ret = (SimpleType) SimpleType.Float32;
			break;
		case ("TLeafD"):
			ret = (SimpleType) SimpleType.Float64;
			break;
		}
		
		// Do I later want to separate fixed and not-fixed arrays?
		if ( ((int) data.getScalar("fLen").getVal() > 1) ||
				(data.getProxy("fLeafCount").getDataSize() != 0)){
			ret = (SimpleType) new SimpleType.ArrayType(ret);
		}
	
		if (ret == null) {
			throw new RuntimeException("Unknown TLeaf type. Class name: " + data.className);
		}
		return ret;
	}
}
