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
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.workloads.DistributionDistance;

/*
 * The ExistingNominalDesigner class takes a set of SQL queries and simply invokes the current DBMS's nominal designer to find the best (nominal) design
 * for these queries. 
 */
public class ExistingNominalDesigner extends RealisticDesignAlgorithm {

	public ExistingNominalDesigner(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designMode, ExperimentCache experimentCache) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
	}

	@Override
	protected PhysicalDesign internalDesign(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {

		PhysicalDesign nominalDesign = dbDesigner.findDesignWithoutWeight(pastWindowQueries, designParameters);

		return nominalDesign;
	}

	@Override
	protected String signature() {
		return ""; // since there's no parameters to return!
	}

}
