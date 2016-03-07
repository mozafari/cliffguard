package edu.umich.robustopt.algorithms;

import java.util.List;

import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.workloads.DistributionDistance;

public interface RequiresPerformanceRecords {
	public PhysicalDesign designUsingPerformanceRecords(List<PerformanceRecord> pastWindowPerformanceRecord, DistributionDistance distributionDistance) throws Exception;
}
