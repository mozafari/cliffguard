package edu.umich.robustopt.algorithms;

import java.util.List;



import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.workloads.DistributionDistance;

/*
 * This class is any designer that magically knows the target workload (called currentWindow) for which it is meant to find the best design.
 * This is in contrast to the RealisticDesignAlgorithm which only knows the past workload (i.e., the window before the target workload)
 * Thus, the purpose of an IdealDesignAlgorithm is only to serve as an upper bound on the best quality that any practical designer can hope
 * for.
 */
public abstract class IdealDesignAlgorithm extends DesignAlgorithm {
	public IdealDesignAlgorithm(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer,
			DesignParameters designMode, ExperimentCache experimentCache) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		// TODO Auto-generated constructor stub
	}

	public abstract PhysicalDesign design(List<PerformanceRecord> currentWindowPerformanceRecords) throws Exception;
}
