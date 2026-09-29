package edu.umich.robustopt.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BLog implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5772818194609055724L;
	private transient List<String> allMessages = new ArrayList<String>(); 
	
	
	public enum LogLevel {
		DEBUG,
		VERBOSE,
		WARNING,
		STATUS,
		BUG
	}
	
	private PrintStream errorFile;
	private PrintStream infoFile;
	
	public LogLevel verbosity;
	
	public BLog() {
		this.errorFile = System.err;
		this.infoFile = System.out;
		this.verbosity = LogLevel.WARNING;
	}
	
	public BLog(LogLevel verbosity) {
		this.errorFile = System.err;
		this.infoFile = System.out;
		this.verbosity = verbosity;
	}

	public BLog(String outputFileName, String errorFileName, LogLevel verbosity) throws FileNotFoundException {
		this.errorFile = new PrintStream(errorFileName);
		this.infoFile = new PrintStream(outputFileName);
		this.verbosity = verbosity;
	}
	
	public void error(String msg) {
		allMessages.add("[ERROR] " + msg + "\n");
		errorFile.println("[ERROR] " + msg);
	}
		
	public void status(LogLevel importance, String msg) {
		if (importance.compareTo(verbosity) >= 0) {
			allMessages.add("["+ importance.toString() +"] " + msg + "\n");
			infoFile.println("["+ importance.toString() +"] " + msg);
		}
	}
	
	public void statusNoNewLine(LogLevel importance, String msg) {
		if (importance.compareTo(verbosity) >= 0) {
			allMessages.add("["+ importance.toString() +"] " + msg + " ");
			infoFile.print("["+ importance.toString() +"] " + msg + " ");
		}
	}
	
	public int getNextIndex() {
		return allMessages.size();
	}
	
	public String getMessagesFromIndex(int startingIndexInclusive) {
		StringBuilder stringBuilder = new StringBuilder();
		
		for (int i=startingIndexInclusive; i<allMessages.size(); ++i)
			stringBuilder.append(allMessages.get(i));
		
		return stringBuilder.toString();
	}
	
	public String getMessagesFromIndexToIndex(int startingIndexInclusive, int endIndexExclusive) {
		StringBuilder stringBuilder = new StringBuilder();
		
		for (int i=startingIndexInclusive; i<endIndexExclusive; ++i)
			stringBuilder.append(allMessages.get(i));

		return stringBuilder.toString();
	}
}
