/**
 * Constructed by IOFactory for non-URL pathnames (e.g. local files)
 */

package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class NIOFile implements FileInterface {
    private RandomAccessFile fh;
    private FileChannel channel;

    public NIOFile(String path) throws FileNotFoundException {
        this.fh = new RandomAccessFile(path, "r");
        this.channel = fh.getChannel();
    }

    @Override
    public ByteBuffer read(long offset, long len) throws IOException {
        int shortLen = (int) len;
        if (shortLen != len) {
            throw new IllegalArgumentException("Attempting to read > 2GBytes");
        }
        ByteBuffer ret = ByteBuffer.allocate(shortLen);
        if (this.channel.read(ret, offset) != shortLen) {
            throw new IOException("Short read");
        }
        return ret;
    }

    @Override
    public ByteBuffer[] readv(int[] offsets, int[] lens) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<ByteBuffer> readAsync(int offset, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<ByteBuffer>[] readvAsync(int[] offsets, int[] lens) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        fh.close();
    }

    @Override
    public long getLimit() throws IOException {
        return fh.length();
    }

    public static List<String> expandPathsToList(String[] paths) throws IOException {
	ArrayList<String> out = new ArrayList<String>();
        for(int i = 0; i < paths.length; ++i) {
	    out.addAll(NIOFile.expandPathToList(paths[i]));
	}
        return out;
    }

    public static List<String> expandPathToList(String path) throws IOException {
        File tmp = FileSystems.getDefault().getPath(path).toFile();
        if (!tmp.isDirectory()) {
            ArrayList<String> ret = new ArrayList<String>();
            ret.add(path);
            return ret;
        } else {
            try (Stream<Path> walk = Files.walk(Paths.get(path))) {
                List<String> result = walk.filter(Files::isRegularFile)
                                            .map(x -> x.toString())
                                            .filter(f -> f.endsWith(".root"))
                                            .collect(Collectors.toList());
                return result;
            }
        }
    }
}
