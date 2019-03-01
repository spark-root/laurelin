/**
 * Factory object to load concrete IO implementations based on path
 */

package edu.vanderbilt.accre.root_proxy;

import java.util.regex.Pattern;

public class IOFactory {
	public static FileInterface openForRead(String path) throws Exception {
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