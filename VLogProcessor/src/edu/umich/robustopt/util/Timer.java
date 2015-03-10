package edu.umich.robustopt.util;

import java.sql.ResultSet;

public class Timer {
	
	private long startTime;
	
	public Timer() {
		this.startTime = System.nanoTime();
	}
	
	public long lapNanos() {
		long now = System.nanoTime();
		long ret = now - startTime;
		return ret;
	}
	
	public double lapMillis() {
		return TimeUtils.NanoSecondsToMillis(lapNanos());
	}
	
	public double lapSeconds() {
		return TimeUtils.NanoSecondsToMillis(lapNanos()) / 1000.0;
	}
	
	public double lapMinutes() {
		return TimeUtils.NanoSecondsToMillis(lapNanos()) / 60000.0;
	}

	public long lapNanosAndReset() {
		long now = System.nanoTime();
		long ret = now - startTime;
		startTime = now;
		return ret;
	}
	
	public double lapMillisAndReset() {
		return TimeUtils.NanoSecondsToMillis(lapNanosAndReset());
	}
	
	public double lapSecondsAndReset() {
		return TimeUtils.NanoSecondsToMillis(lapNanosAndReset()) / 1000.0;
	}
	
	public double lapMinutesAndReset() {
		return TimeUtils.NanoSecondsToMillis(lapNanosAndReset()) / 60000.0;
	}

}
