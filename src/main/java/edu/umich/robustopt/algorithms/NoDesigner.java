package edu.umich.robustopt.algorithms;

import java.util.ArrayList;
import java.util.List;



import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.workloads.DistributionDistance;

/* 
 * The NoDesigner class is a trivial designer that simply returns an empty design. This class provides an easy way of
 * finding out what the performance would have been if there no projects at all, i.e. if we were to use the super-projection
 * to process all the queries. 
 */
public class NoDesigner extends RealisticDesignAlgorithm {

	public NoDesigner(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designMode, ExperimentCache experimentCache) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
	}

	@Override
	public PhysicalDesign internalDesign(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {
		PhysicalDesign emptyDesign = new PhysicalDesign (new ArrayList<PhysicalStructure>());
		return emptyDesign;
	}
	
	public static void main(String[] argv) {
		
	}

	@Override
	protected String signature() {
		// since there's no params!
		return "";
	}

}
