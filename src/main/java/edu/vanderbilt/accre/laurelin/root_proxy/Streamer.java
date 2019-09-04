package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Streamer {
    /*
     * Doesn't exist in ROOT, encapsulates Streamer functionality from all the different subclasses
     */
    ArrayList<Proxy> streamerList;
    HashMap<String, Proxy> streamerMap;
    HashMap<Long, Proxy> classMap;

    public void getFromCursor(Cursor c, long off) throws IOException {
        /*
         * This is used to parse the toplevel Streamer info from ROOT, e.g.
         * TFile->fSeekInfo
         *
         *
         * https://github.com/scikit-hep/uproot/blob/662d1f859f8ba7a5d908a249b3cae5b743e56a19/uproot/rootio.py#L521
         *
         */
        streamerList = new ArrayList<Proxy>();
        TList listInfo = new TList();
        RangeCheck check = new RangeCheck(c);
        listInfo.readFromCursor(c.getSubcursor(off));

        int count = listInfo.size();
        Cursor dataCursor = listInfo.getDataCursor();
        ProxyArray arr = new ProxyArray();
        arr.createPlace = "getfromcursor";
        classMap = new HashMap<Long, Proxy>();
        for (int i = 0; i < count; i += 1) {
            Proxy data = readObjAny(dataCursor, classMap);
            arr.add(data);
            short n = dataCursor.readUChar();
            dataCursor.setOffset(dataCursor.getOffset() + n);
        }
        check.verify(dataCursor);

        streamerMap = new HashMap<String, Proxy>();
        for (Proxy p : arr) {
            String name = (String)p.getScalar("fName").getVal();
            streamerMap.put(name, p);
            streamerList.add(p);
        }
    }

    /*
     * Deserializes an object described with the given TKey
     * @param key
     * @param c
     * @return
     * @throws IOException
     */
    public Proxy deserializeWithStreamer(TKey key, Cursor c) throws IOException {
        Proxy ret = new Proxy();
        ret.createPlace = "deserializeWithStreamer";
        return deserializeWithStreamerImpl(key.fClassName, c, ret);
    }

    public Proxy deserializeWithStreamer(String fClassName, Cursor c) throws IOException {
        Proxy ret = new Proxy();
        ret.createPlace = "deserializeWithStreamer2";
        return deserializeWithStreamerImpl(fClassName, c, ret);
    }

    private Proxy deserializeWithStreamerImpl(String fClassName, Cursor c, Proxy ret) throws IOException {
        if (fClassName.equals("TObjArray")) {
            return deserializeTObjArray(c, ret);
        } else if (fClassName.startsWith("TArray")) {
            return deserializeTArray(c, ret, fClassName);
        } else if (fClassName.contentEquals("ROOT::TIOFeatures")) {
            /*
             * These objects store nothing useful and don't follow the regular
             * streamer description, so lets just fast forward and skip it
             */
            RangeCheck check = new RangeCheck(c);
            c.setOffset(check.getStart() + check.getCount());
            return new ProxyElement<String>("TIOFeatures-Ignored");
        }

        RangeCheck check = new RangeCheck(c);
        Proxy streamer = getStreamer(fClassName, check.getVers());
        ProxyArray fElements = (ProxyArray) streamer.getProxy("fElements");
        Proxy subObj;
        for (Proxy ele: fElements) {
            switch (ele.className) {
                case "TStreamerBase":
                    ret = deserializeWithStreamerImpl((String) ele.getScalar("fName").getVal(), c, ret);
                    break;
                case "TStreamerBasicType":
                    ret = deserializeStreamerBasicType(ele, c, ret);
                    break;
                case "TStreamerString":
                    ret = deserializeStreamerString(ele, c, ret);
                    break;
                case "TStreamerBasicPointer":
                    ret = deserializeStreamerBasicPointer(ele, c, ret);
                    break;
                case "TStreamerObject":
                case "TStreamerObjectAny":
                    if (fClassName.equals("TBranch") && ((String) ele.getScalar("fName").getVal()).equals("fBaskets")) {
                        // The baskets inside a TBranch aren't useful
                        RangeCheck check2 = new RangeCheck(c);
                        c.setOffset(check2.getStart() + check2.getCount());
                        continue;
                    }
                    subObj = new Proxy();
                    subObj.createPlace = "tstreamerobj";
                    subObj = deserializeWithStreamerImpl((String) ele.getScalar("fTypeName").getVal(), c, subObj);
                    subObj.setClass((String) ele.getScalar("fTypeName").getVal());
                    ret.putProxy((String) ele.getScalar("fName").getVal(), subObj);
                    break;
                case "TStreamerObjectPointer":
                    subObj = new Proxy();
                    subObj.createPlace = "tstreamerobjpointer";
                    subObj = deserializeTStreamerObjectPointer(ele, c, subObj);
                    String fName = (String) ele.getScalar("fName").getVal();
                    ret.putProxy(fName, subObj);
                    if (subObj == null) {
                        break;
                    }
                    Integer fType = (Integer) ele.getScalar("fType").getVal();
                    String fTypeName = (String) ele.getScalar("fTypeName").getVal();
                    if (subObj.getClassName().equals("") && fTypeName != null) {
                        subObj.setClass(fTypeName);
                    } else if (subObj.getClassName().equals("") && fType != null) {
                        subObj.setClass("" + ele.getScalar("fType").getVal());
                    }
                    break;
                default:
                    throw new IOException("Unknown TStreamerElement type " + ele.className);
            }
        }
        if (!fClassName.equals("TSeqCollection") && !fClassName.contentEquals("TObjArray")) {
            check.verify(c);
        }

        ret.setClass(fClassName);
        return ret;
    }

    private Proxy deserializeTArray(Cursor c,Proxy ret,String fClassName) throws IOException {
        int length = c.readInt();

        switch (fClassName) {
            case "TArrayD":
                ProxyElement<double []> dret = new ProxyElement<double []>();
                double[] dval = c.readDoubleArray(length);
                dret.setVal(dval);
                return dret;
            case "TArrayI":
                ProxyElement<int []> iret = new ProxyElement<int []>();
                int[] ival = c.readIntArray(length);
                iret.setVal(ival);
                return iret;
            default:
                throw new IOException("Unknown TArray type: " + fClassName);
        }
    }

    private Proxy deserializeTStreamerObjectPointer(Proxy ele, Cursor c, Proxy ret) throws IOException {
        // if element._fType == kObjectp or element._fType == kAnyp:
        int fType = (int) ele.getScalar("fType").getVal();
        if ((fType == 63) || (fType == 68)) {
            throw new IOException("Unsupported fType in object pointer " + fType);
        } else if ((fType == 64) || (fType == 69)) {
            // if element._fType == kObjectP or element._fType == kAnyP:
            ret = readObjAny(c, classMap);
            if (ret == null) {
                ret = new Proxy();
                ret.createPlace = "deserializeWithtStreamerobjpointer";
                ret.setClass((String) ele.getScalar("fTypeName").getVal());
            }
        } else {
            throw new IOException("Unsupported fType in object pointer " + fType);
        }

        return ret;

    }

    private Proxy deserializeTObjArray(Cursor c, Proxy ret) throws IOException {
        ProxyArray arrret = new ProxyArray();
        RangeCheck check = new RangeCheck(c);
        parseTObject(c, arrret);
        String fName = c.readTString();
        ret.putScalar("fName", fName);
        int size = c.readInt();
        int low = c.readInt();
        //System.out.println("tobjarray " + fName + " size " + size + " low " + low);
        for (int i = 0; i < size; i += 1) {
            Cursor preC = c.duplicate();
            Proxy test = readObjAny(c, classMap);
            //System.out.println( "    read " + test);
            if (test == null) {
                test = new Proxy();
                test.createPlace = "tobjarraydeserialize";
                test.setClass("TObjArrayUnknown");
            }

            arrret.add(test);
        }

        check.verify(c);
        ret = arrret;
        return arrret;
    }

    private Proxy deserializeStreamerBasicPointer(Proxy ele, Cursor c, Proxy ret) throws IOException {
        // Don't know what to do with these yet
        /*
        assert uproot.const.kOffsetP < element._fType < uproot.const.kOffsetP + 20
        fType = element._fType - uproot.const.kOffsetP

        dtypename = "_dtype{0}".format(len(dtypes) + 1)
        dtypes[dtypename] = _ftype2dtype(fType)

        code.append("        fBasketSeek_dtype = cls.{0}".format(dtypename))
        if streamerinfo._fName == b"TBranch" and element._fName == b"fBasketSeek":
            code.append("        if getattr(context, \"speedbump\", True):")
            code.append("            if cursor.bytes(source, 1)[0] == 2:")
            code.append("                fBasketSeek_dtype = numpy.dtype('>i8')")
        else:
            code.append("        if getattr(context, \"speedbump\", True):")
            code.append("            cursor.skip(1)")

        code.append("        self._{0} = cursor.array(source, self._{1}, fBasketSeek_dtype)".format(_safename(element._fName), _safename(element._fCountName)))
        fields.append(_safename(element._fName))
        recarray.append("raise ValueError('not a recarray')")
         */
        //ele.dump();

        // https://github.com/cxx-hep/root-cern/blob/87292c7e536c606c81addb1979ea2758f49e5fc4/io/io/src/TStreamerInfoReadBuffer.cxx#L67
        c.readChar();
        int fType = (int) ele.getScalar("fType").getVal() - Constants.kOffsetP;
        String fCountName = (String) ele.getScalar("fCountName").getVal();
        int count = (int) ret.getScalar(fCountName).getVal();
        String fName = (String) ele.getScalar("fName").getVal();
        if (fType == 16) {
            // kLong64
            ret.putScalar(fName, c.readLongArray(count));
        } else if (fType == 3) {
            // kInt
            ret.putScalar(fName, c.readIntArray(count));
        } else {
            throw new IOException("oops");
        }

        return ret;
    }

    private Proxy deserializeStreamerString(Proxy ele, Cursor c, Proxy ret) throws IOException {
        String fName = (String) ele.getScalar("fName").getVal();
        ret.putScalar(fName, c.readTString());
        return ret;
    }

    private Proxy deserializeStreamerBasicType(Proxy ele, Cursor c, Proxy ret) throws IOException {
        int fArrayLength = (int) ele.getScalar("fArrayLength").getVal();
        if (fArrayLength != 0) { throw new IOException("oops"); }
        int fType = (int) ele.getScalar("fType").getVal();
        String fName = (String) ele.getScalar("fName").getVal();
        switch (fType) {
            case 1:
                // kChar
                ret.putScalar(fName,  c.readChar());
                break;
            case 2:
                // kShort
                ret.putScalar(fName, c.readShort());
                break;
            case 3:
                // kInt
                ret.putScalar(fName, c.readInt());
                break;
            case 5:
                // kFloat
                ret.putScalar(fName, c.readFloat());
                break;
            case 6:
                // kCounter
                ret.putScalar(fName, c.readInt());
                break;
            case 8:
                // kDouble
                ret.putScalar(fName,  c.readDouble());
                break;
            case 11:
                // kUChar
                ret.putScalar(fName,  c.readUChar());
                break;
            case 13:
                // kUInt
                ret.putScalar(fName, c.readUInt());
                break;
            case 15:
                // kBits
                ret.putScalar(fName, c.readUInt());
                break;
            case 16:
                // kLong64
                ret.putScalar(fName,  c.readLong());
                break;
            case 18:
                // kBool
                ret.putScalar(fName,  c.readChar());
                break;
            default:
                throw new IOException("Unknown basic type");
        }
        return ret;
    }

    private Proxy getStreamer(String name, int version) throws IOException {
        Proxy ret = streamerMap.get(name);
        if (!streamerMap.containsKey(name)) {
            throw new IOException("Streamer not found");
        }
        int streamerVers = (int) ret.getScalar("fClassVersion").getVal();
        if (streamerVers == version) {
            return ret;
        }

        // Search the whole list for the correct streamer corresponding to the requested version
        for (Proxy streamer: streamerList) {
            String streamerName = (String) streamer.getScalar("fName").getVal();
            ProxyElement<Integer> streamerEle = streamer.getScalar("fClassVersion");
            if ((streamerEle == null) && (streamerName.equals(name))) {
                return streamer;
            } else if ((streamerEle == null)) {
                continue;
            }
            streamerVers = streamerEle.getVal();

            if ((streamerName.equals(name)) && (streamerVers == version)) {
                return streamer;
            }
        }
        return ret;
    }

    // HACK - dedup with ClassDeserializer
    private void parseTObject(Cursor cursor, Proxy target) throws IOException {
        // Stolen from uproot
        short version;
        long fBits;
        version = cursor.readShort();
        if ((version & Constants.kByteCountVMask) != 0)
            cursor.skipBytes(4);
        target.putScalar("version", version);
        target.putScalar("fUniqueID", cursor.readUInt());
        fBits = cursor.readUInt();
        fBits = fBits | Constants.kIsOnHeap;

        if ((fBits & Constants.kIsReferenced) != 0)
            cursor.skipBytes(2);
        target.putScalar("fBits", fBits);
        target.setClass("TObject");
    }

    public Proxy readObjAny(Cursor cursor, HashMap<Long,Proxy> classMap) throws IOException {
        // follows uproot
        // https://github.com/scikit-hep/uproot/blob/662d1f859f8ba7a5d908a249b3cae5b743e56a19/uproot/rootio.py#L428
        // and root4j
        // https://github.com/diana-hep/root4j/blob/2a7bd47582755a5bc85dd8b05ed5ee7d9fecf6f6/src/main/java/org/dianahep/root4j/core/RootInputStream.java#L475
        Cursor startCursor = cursor.duplicate();
        long vers;
        long start;
        long tag;
        long beg = cursor.getOffset() - cursor.getOrigin();
        long bcnt = cursor.readUInt();

        if (((bcnt & Constants.kByteCountMask) == 0) || (bcnt == Constants.kNewClassTag)) {
            vers = 0;
            start = 0;
            tag = bcnt;
            bcnt = 0;
        } else {
            vers = 1;
            start = cursor.getOffset() - cursor.getOrigin();
            tag = cursor.readUInt();
        }

        if ((tag & Constants.kClassMask) == 0) {
            // Reference Object
            if (tag == 0) {
                return null;
            } else if (tag == 1) {
                // FixMe: tag == 1 means "self", but don't currently have self available.
                return null;
            }
            Object obj = classMap.get(new Long(tag));
            if ((obj == null) || !(obj instanceof Proxy)) {
                cursor.setOffset(startCursor.getOrigin() + beg + bcnt + 4);
                ProxyElement<String> ret = new ProxyElement<String>();
                ret.setVal(String.format("Unknown object at tag %s", tag));
                return ret;
            }
            return (Proxy) obj;
        } else if (tag == Constants.kNewClassTag) {
            //System.out.println(" break2 tag " + tag + " class tag " + Constants.kNewClassTag);
            // New class and object
            String cname = cursor.readCString();
            //GenericRootClass rootClass = (GenericRootClass) in.getFactory().create(className);
            ClassDeserializer rootClass = new ClassDeserializer(cname, this);

            // Add this class to the map
            long classKey;
            if (vers > 0) {
                classKey = start + Constants.kMapOffset;
            } else {
                classKey = classMap.size() + 1;
            }

            //System.out.println("key1 " + classKey);
            classMap.put(new Long(classKey), rootClass);

            Proxy realRootClass = rootClass.read(cursor, classMap);

            // Add this instance to the map
            if (vers > 0) {
                classKey = beg + Constants.kMapOffset;
            } else {
                classKey = classMap.size() + 1;
            }

            assert realRootClass != null;

            //System.out.println("key2 " + classKey);
            classMap.put(new Long(classKey), realRootClass);

            // realRootClass.readFromFile()
            // obj.read(in);
            return realRootClass;
        } else {
            // new object from existing class
            tag &= ~Constants.kClassMask;

            ClassDeserializer cls = (ClassDeserializer) classMap.get(new Long(tag));
            if ((cls == null) || !(cls instanceof Proxy)) {
                throw new IOException("Invalid object tag " + tag);
            }

            Proxy instance = cls.read(cursor, classMap);
            assert instance != null;

            if (vers > 0) {
                Long offset = new Long(beg + Constants.kMapOffset);
                //System.out.println("key3 " + offset);
                classMap.put(offset, instance);

            } else {
                Long offset = new Long(classMap.size() + 1);
                classMap.put(offset, instance);
            }

            // realRootClass.readFromFile()
            // obj.read(in);
            return instance;
        }
    }
}
