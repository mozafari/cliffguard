package edu.umich.robustopt.common;

import java.io.File;

public class GlobalConfigurations {
	public static final String RO_BASE_PATH = System.getProperty("user.home") + File.separator + "robust-opt";
	public static final String RO_BASE_CACHE_PATH = RO_BASE_PATH + File.separator+ "cached";
	public static final Long RANDOM_SEED = (long) 1234567890;
	public static final int SynchronizationFrequencyForPerformanceRecords = 50;
	public static final int SynchronizationFrequencyForDesigns = 1;
	
}
