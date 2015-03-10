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
 * FutureKnowingNominalDesignAlgorithm is an ideal designer (it exactly knows the target workload) which then simply 
 * invokes the current DBMS's designer to find the best design for this future workload (as opposed to say, solving an ILP to find the best
 * design for the future workload).
 * 
 */
public class FutureKnowingNominalDesignAlgorithm extends IdealDesignAlgorithm {

	public FutureKnowingNominalDesignAlgorithm(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designMode,
			ExperimentCache experimentCache) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		// TODO Auto-generated constructor stub
	}

	@Override
	public PhysicalDesign design(List<PerformanceRecord> currentWindowPerformanceRecords) throws Exception {
		log.status(LogLevel.VERBOSE, "designing using FutureKnowingNominalDesignAlgorithm ...\n");
		List<String> currentWindow = convertPerformanceRecordToQueries(currentWindowPerformanceRecords);
		Timer t = new Timer();
		Timer designTimer = new Timer(); int logIndex = log.getNextIndex();
		PhysicalDesign nominalDesign = dbDesigner.findDesignWithoutWeight(currentWindow , designParameters);
		if (nominalDesign == null)
			throw new RuntimeException("no physical structures found!");
		log.status(LogLevel.VERBOSE, "Designed using FutureKnowingNominalDesignAlgorithm " + nominalDesign.getPhysicalStructuresAsList().size() + " nominal physical structures in " + t.lapMinutes() + " minutes.\n");
		for (int view=0; view<nominalDesign.getPhysicalStructures().size(); ++view)
			log.status(LogLevel.DEBUG, "struct_id=" + view + ": "+nominalDesign.getPhysicalStructuresAsList().get(view).getHumanReadableSummary());
		
		return nominalDesign;
	}

}
