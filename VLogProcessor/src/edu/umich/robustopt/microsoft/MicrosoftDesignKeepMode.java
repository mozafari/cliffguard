package edu.umich.robustopt.microsoft;

public enum MicrosoftDesignKeepMode {
	ALL,	// Kepp all existing physical design structures
	NONE,	// Do not keep any existing physical design structures
	CL_IDX,	// Keep clustered indexed
	IDX,	// Keep clustered and nonclucstered indexes
	ALIGNED	// Kepp aligned partitioning
}
