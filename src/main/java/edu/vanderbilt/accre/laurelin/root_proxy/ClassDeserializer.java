package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.HashMap;

public class ClassDeserializer extends Proxy {
    Streamer streamer;

    public ClassDeserializer(String name, Streamer streamer) {
        className = name;
        this.streamer = streamer;
    }

    public Proxy read(Cursor cursor, HashMap<Long,Proxy> classMap) throws IOException {
        Proxy proxy = new Proxy();
        proxy.createPlace = "class deserializer";
        proxy.setDeserializer(this);

        Proxy newproxy = handleBuiltin(cursor, proxy, classMap);
        if (newproxy == null) {
            // If we can't process it with the builtins, skip it
            //RangeCheck check = new RangeCheck(cursor);
            proxy = streamer.deserializeWithStreamer(className, cursor);
            //check.verify(cursor);
            return proxy;
        }
        newproxy.className = className;
        return newproxy;
    }

    private Proxy handleBuiltin(Cursor cursor, Proxy target, HashMap<Long,Proxy> classMap) throws IOException {
        return handleBuiltin(cursor, target, classMap, className);
    }

    private Proxy handleBuiltin(Cursor cursor, Proxy target, HashMap<Long,Proxy> classMap, String className) throws IOException {
        RangeCheck check;
        ProxyArray arrtarget;
        ProxyElement<String> stringtarget;

        int size;
        switch (className) {
            case "TList":
                check = new RangeCheck(cursor);
                arrtarget = new ProxyArray();
                arrtarget.createPlace = "tlistbuiltin";
                parseTObject(cursor, arrtarget);
                arrtarget.putScalar("fName", cursor.readTString());
                size = cursor.readInt();
                arrtarget.putScalar("fSize", size);
                for (int i = 0; i < size; i += 1) {
                    arrtarget.add(streamer.readObjAny(cursor, classMap));
                    // https://github.com/scikit-hep/uproot/blob/662d1f859f8ba7a5d908a249b3cae5b743e56a19/uproot/rootio.py#L1299
                    short n = cursor.readUChar();
                    cursor.setOffset(cursor.getOffset() + n);
                }
                target = arrtarget;
                check.verify(cursor);

                break;
            case "TObjString":
                check = new RangeCheck(cursor);
                stringtarget = new ProxyElement<String>();
                stringtarget.createPlace = "tobjstringbuiltin";
                parseTObject(cursor, stringtarget);
                String stringbuf = cursor.readTString();
                stringtarget.setVal(stringbuf);
                check.verify(cursor);
                break;
            case "TStreamerInfo":
                check = new RangeCheck(cursor);
                parseTNamed(cursor, target);
                target.putScalar("checksum", cursor.readUInt());
                target.putScalar("fClassVersion", cursor.readInt());
                target.putProxy("fElements", streamer.readObjAny(cursor, classMap));
                check.verify(cursor);
                break;
            case "TObjArray":
                check = new RangeCheck(cursor);
                arrtarget = new ProxyArray();
                arrtarget.createPlace = "tobjarraybuiltin";
                parseTObject(cursor, target);
                arrtarget.putScalar("fName", cursor.readTString());
                size = cursor.readInt();
                arrtarget.putScalar("size", size);
                arrtarget.putScalar("low", cursor.readInt());
                for (int i = 0; i < size; i += 1) {
                    arrtarget.add(streamer.readObjAny(cursor, classMap));
                }
                target = arrtarget;
                check.verify(cursor);
                break;
            case "TStreamerElement":
                check = new RangeCheck(cursor);
                parseTNamed(cursor, target);
                int fType = cursor.readInt();
                target.putScalar("fType", fType);
                target.putScalar("fSize", cursor.readInt());
                target.putScalar("fArrayLength", cursor.readInt());
                target.putScalar("fArrayDim", cursor.readInt());
                int classversion = check.getVers();

                /* skip maxindex because we don't need it */
                int n = 5;
                if (classversion == 1) {
                    n = cursor.readUChar();
                }
                cursor.setOffset(cursor.getOffset() + (n * 4));

                target.putScalar("fTypeName", cursor.readTString());
                /*
                 * uproot does some tidying up too
                 */

                if (classversion == 3) {
                    throw new IOException("classversion 3 isn't supported");
                }
                check.verify(cursor);
                break;
            case "TStreamerBase":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                if (check.getVers() >= 2) {
                    target.putScalar("fBaseVersion", cursor.readInt());
                }
                check.verify(cursor);
                break;
            case "TStreamerBasicType":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerBasicPointer":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                target.putScalar("fCountVersion", cursor.readInt());
                target.putScalar("fCountName", cursor.readTString());
                target.putScalar("fCountCycle", cursor.readTString());
                check.verify(cursor);
                break;
            case "TStreamerObject":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerObjectPointer":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerObjectAny":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerObjectAnyPointer":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerString":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                check.verify(cursor);
                break;
            case "TStreamerSTL":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerElement");
                target.putScalar("fSTLType", cursor.readInt());
                target.putScalar("fCtype", cursor.readInt());
                check.verify(cursor);
                break;
                // Are there really both capitalizations? I needed to add the lowercase
                // version to parse NANOAOD, but I don't remember if the uppercase
                // version is also needed
                //case "TStreamerSTLString":
            case "TStreamerSTLstring":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerSTL");
                check.verify(cursor);
                break;
            case "TStreamerArtificial":
                check = new RangeCheck(cursor);
                target = handleBuiltin(cursor, target, classMap, "TStreamerSTL");
                check.verify(cursor);
                break;
            default:
                return null;
        }
        return target;
    }

    private void parseTNamed(Cursor cursor, Proxy target) throws IOException {
        RangeCheck check = new RangeCheck(cursor);
        parseTObject(cursor, target);
        target.putScalar("fName", cursor.readTString());
        target.putScalar("fTitle", cursor.readTString());
        check.verify(cursor);
    }

    private void parseTObject(Cursor cursor, Proxy target) throws IOException {
        // Stolen from uproot
        short version;
        long fBits;
        version = cursor.readShort();
        if ((version & Constants.kByteCountVMask) != 0) {
            cursor.skipBytes(4);
        }
        target.putScalar("version", version);
        target.putScalar("fUniqueID", cursor.readUInt());
        fBits = cursor.readUInt();
        fBits = fBits | Constants.kIsOnHeap;

        if ((fBits & Constants.kIsReferenced) != 0) {
            cursor.skipBytes(2);
        }
        target.putScalar("fBits", fBits);
    }
}
