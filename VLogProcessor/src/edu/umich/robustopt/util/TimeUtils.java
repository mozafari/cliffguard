package edu.umich.robustopt.util;

public class TimeUtils {
	public static double NanoSecondsToMillis(long nanos) {
		return ((double) nanos) / 1e6;
	}
}
