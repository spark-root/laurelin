/**
 * Factory object to load concrete IO implementations based on path
 */

package edu.vanderbilt.accre.laurelin.root_proxy;

import java.io.IOException;
import java.util.regex.Pattern;

public class IOFactory {
	public static FileInterface openForRead(String path) throws IOException {
		/**
		 * Depending on the incoming path, load an implementation
		 */
		if (Pattern.matches("^[a-zA-Z]+:/.*", path)) {
			return new HadoopFile(path);			
		} else {
			return new NIOFile(path);
		}
	}
}
