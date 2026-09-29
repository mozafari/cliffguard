package edu.umich.robustopt.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StringUtils {
	
	public static String Join(List<String> elems, String sep) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < elems.size(); i++) {
			sb.append(elems.get(i));
			if ((i + 1) != elems.size())
				sb.append(sep);
		}
		return sb.toString();
	}
	
	/*
	 * maximumLines=-1 means read all the lines!
	 */
	public static List<String> GetLinesFromFile(File f, int maximumLines) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		List<String> lines = new ArrayList<String>();
		String line;
		while ((line = br.readLine()) != null) {
			if (maximumLines>=0 && lines.size() >= maximumLines)
				break;
			lines.add(line);
		}
		br.close();
		return lines;
	}

	public static List<String> GetLinesFromFile(File f) throws IOException {
		return GetLinesFromFile(f, -1);
	}

	
	// the following below are what happens when you don't 
	// have a language with proper closures...
	
	public static <T> List<String> ElemStringify(List<T> elems) {
		List<String> l = new ArrayList<String>();
		for (T t : elems)
			l.add(t.toString());
		return l;
	}
	
	public static List<String> ElemSQLSingleQuote(List<String> elems) {
		// TODO: proper escaping!
		List<String> l = new ArrayList<String>();
		for (String e : elems)
			l.add("'" + e + "'");
		return l;
	}

	public static String rtrim(String s) {
		int x = s.length() - 1;
		while (x >= 0 && Character.isWhitespace(s.charAt(x)))
			x--;
		return s.substring(0, x + 1);
	}

}
