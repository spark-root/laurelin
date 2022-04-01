package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;

import edu.vanderbilt.accre.laurelin.root_proxy.io.Cursor;
import edu.vanderbilt.accre.laurelin.root_proxy.io.ROOTFile;

public class TKey {
    int Nbytes;
    int version;
    int ObjLen;
    int KeyLen;
    int Cycle;
    long fSeekKey;
    long fSeekPdir;
    String fClassName;
    String fName;
    String fTitle;

    public Cursor getFromFile(ROOTFile fh, long off) throws IOException {
        Cursor buffer = fh.getCursor(off);
        return getFromFile(buffer);
    }

    public Cursor getFromFile(Cursor buffer) throws IOException {
        long tmpoff = buffer.getOffset() + buffer.getBase();
        Nbytes = buffer.readInt();
        version = buffer.readShort();
        ObjLen = buffer.readInt();
        // https://root.cern.ch/doc/v614/classTDatime.html
        // This class stores the date and time with a precision of one second in an unsigned 32 bit word (950130 124559).
        long dummy = buffer.readInt();

        KeyLen = buffer.readShort();
        Cycle = buffer.readShort();
        if (version > 1000) {
            fSeekKey = buffer.readLong();
            fSeekPdir = buffer.readLong();
        } else {
            fSeekKey = buffer.readInt();
            fSeekPdir = buffer.readInt();
        }

        // Get strings
        fClassName = buffer.readTString();
        fName = buffer.readTString();
        fTitle = buffer.readTString();
        return buffer;
    }

    public int getKeyLen() {
        return KeyLen;
    }

    public int getNBytes() {
        return Nbytes;
    }

    public int getObjLen() {
        return ObjLen;
    }

    public String getClassName() {
        return fClassName;
    }
}
