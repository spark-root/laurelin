package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.tukaani.xz.XZInputStream;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class Compression {
	/*
	 * Wraps ROOT's "blocked compression" algo
	 */
	public enum ZAlgo {
		GLOBAL_SETTING,
		ZLIB,
		LZMA,
		OLD,
		L4,
		UNDEFINED;

		public static ZAlgo getAlgo(int fCompress){
			int algo = (fCompress - (fCompress % 100) ) / 100;
			return ZAlgo.values()[algo];
		}

		public static int getLevel(int fCompress){
			return fCompress % 100;
		}

		public static ZAlgo getAlgo(byte h1, byte h2){
			if(h1 == (byte)'Z' && h2 == (byte)'L'){
				return ZLIB;
			}
			if(h1 == (byte)'X' && h2 == (byte)'Z'){
				return LZMA;
			}
			if(h1 == (byte)'L' && h2 == (byte)'4'){
				return L4;
			}
			return UNDEFINED;
		}
	}
	static short getUChar(ByteBuffer buf, int off) {
		short ret = buf.get(off);
		if (ret < 0) {
			ret += 256;
		}
		return ret;
	}
	static ByteBuffer decompressBytes(ByteBuffer in, int compressedSize, int decompressedSize) throws IOException {
		int HDRSIZE = 9;
		int L4CSUMSIZE = 8;
		// from root4j
		// https://github.com/diana-hep/root4j/blob/2a7bd47582755a5bc85dd8b05ed5ee7d9fecf6f6/src/main/java/org/dianahep/root4j/core/RootInputStream.java#L612
		// Currently we read the whole buffer before starting to decompress.
		// It would be better to decompress each component as we read it, but perhaps
		// not possible if we need to support random access into the unpacked array.
//		try
//		{
			byte[] buf = new byte[compressedSize];
			in.position(0);
			in.get(buf, 0, compressedSize);
			byte[] out = new byte[decompressedSize];
			//byte[] out = new byte[decompressedSize];
			int frameOffset = 0;
			int outOffset = 0;
			//while cursor.index - start < self._compressedbytes:
			while (frameOffset < compressedSize) {
				byte h1 = (byte)in.get(frameOffset + 0);
				byte h2 = (byte)in.get(frameOffset + 1);
				ZAlgo algo = ZAlgo.getAlgo(h1, h2);
				// byte 2 is "method" .. don't know what that means
				int c1 = getUChar(in, frameOffset + 3);
				int c2 = getUChar(in, frameOffset + 4);
				int c3 = getUChar(in, frameOffset + 5);
				int u1 = getUChar(in, frameOffset + 6);
				int u2 = getUChar(in, frameOffset + 7);
				int u3 = getUChar(in, frameOffset + 8);
				int frameCompressedSize = c1 + (c2 << 8) + (c3 << 16);
				int frameDecompressedSize = u1 + (u2 << 8) + (u3 << 16);
				frameOffset += HDRSIZE;
				if (algo == ZAlgo.L4) {
					frameCompressedSize -= L4CSUMSIZE;
					frameOffset += L4CSUMSIZE;
				}
				try {
					decompressFrame(buf, out, algo, frameOffset, outOffset, frameCompressedSize, frameDecompressedSize);
				} catch (DataFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				outOffset += frameDecompressedSize;
				frameOffset += frameCompressedSize;
				
			}
			
			return ByteBuffer.wrap(out);

//		}
//		catch (Exception x)
//		{
//			IOException xx = new IOException("Error during decompression (size="+compressedSize+"/"+decompressedSize+")");
//			xx.initCause(x);
//			throw xx;
//		}
//		catch (OutOfMemoryError x)
//		{
//			IOException xx = new IOException("Error during decompression (size="+compressedSize+"/"+decompressedSize+")");
//			xx.initCause(x);
//			throw xx;         
//		}
	}
	
	static void decompressFrame(byte[] in, byte[] out, ZAlgo algo, int inOffset, int outOffset, int compressedSize, int decompressedSize) throws IOException, DataFormatException {
		int decompressed = -1;
		switch (algo) {
		case ZLIB:
			Inflater inf = new Inflater();
			inf.setInput(in, inOffset, compressedSize);
			decompressed = inf.inflate(out, outOffset, decompressedSize);
			break;
		case LZMA:
			ByteArrayInputStream bufStr = new ByteArrayInputStream(in);
			bufStr.skip(inOffset);
			XZInputStream unc = new XZInputStream(bufStr);
			decompressed = unc.read(out, 0, decompressedSize);
			// Library recommendation for integrity check
			if( unc.read() != -1 || decompressed != decompressedSize){
				throw new IOException( "Failed to decompress all LZMA bytes." );
			}
			break;
		case L4:
			LZ4Factory factory = LZ4Factory.fastestInstance();
			LZ4FastDecompressor decompressor = factory.fastDecompressor();
			decompressor.decompress(in, inOffset, out, outOffset, decompressedSize);
		default:
			throw new IOException( "Unable to determine compression algorithm" );
		}
//		if ((decompressed != -1) && (decompressed != decompressedSize)) {
//			throw new IOException( "Incorrect number of bytes decompressed from frame: " +
//									decompressed + " != " + decompressedSize);
//		}
	}
}
