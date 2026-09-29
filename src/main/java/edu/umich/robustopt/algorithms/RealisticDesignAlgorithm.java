package edu.umich.robustopt.algorithms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;




import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.workloads.DistributionDistance;

/*
 * This class is any designer that can be actually implemented in practice, i.e. a designer that makes a decision by only knowing 
 * the past window (i.e., the window prior to the present window)
 * This is in contrast to the IdealDesignAlgorithm which magically knows the future workload (here, the presentWindow is the target workload)
 */
public abstract class RealisticDesignAlgorithm extends DesignAlgorithm {
	public RealisticDesignAlgorithm(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer,
			DesignParameters designMode, ExperimentCache experimentCache) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		// TODO Auto-generated constructor stub
	}
		
	final public PhysicalDesign design(List<PerformanceRecord> pastWindowPerformanceRecord, DistributionDistance distributionDistance) throws Exception {		
		List<String> pastWindowQueries = convertPerformanceRecordToQueries(pastWindowPerformanceRecord);
		String designerName = this.getClass().getSimpleName();
		
		String sign = (this instanceof RobustDesigner? ((RobustDesigner)this).robustSignature(distributionDistance) : this.signature());
		
		if (experimentCache!=null && experimentCache.getDesign(pastWindowQueries, getName(), sign)!= null) { 
			log.status(LogLevel.VERBOSE, "design fetched from cache for " + getName() + " using " + sign);
			return experimentCache.getDesign(pastWindowQueries, getName(), sign);
		}
		
		log.status(LogLevel.VERBOSE, "designing using " + designerName + " ...\n");
		Timer designTimer = new Timer(); int logIndex = log.getNextIndex();
		PhysicalDesign iDesign = null;
		
		// invoking the internal logic of the designer to choose which projections to return!
		if (this instanceof RequiresPerformanceRecords)
			iDesign = ((RequiresPerformanceRecords)this).designUsingPerformanceRecords(pastWindowPerformanceRecord, distributionDistance);
		else
			iDesign = internalDesign(pastWindowQueries, distributionDistance);
		
		if (iDesign == null)
			throw new RuntimeException("no projections found!");

		log.status(LogLevel.VERBOSE, "Designed (using " + designerName + ") " + iDesign.getPhysicalStructures().size() + " projections in " + designTimer.lapMinutes() + " minutes!\n");
		for (int view=0; view<iDesign.getPhysicalStructuresAsList().size(); ++view)
			log.status(LogLevel.DEBUG, "proj_id=" + view + ": "+iDesign.getPhysicalStructuresAsList().get(view).getHumanReadableSummary());		

		// DY: internalDesign() can cache the design, so added an additional check routine before try caching here.
		if (experimentCache!=null && experimentCache.getDesign(pastWindowQueries, getName(), sign) == null)
			experimentCache.cacheDesignByQueriesAlgorithm(pastWindowQueries, getName(), sign, iDesign, ((long) designTimer.lapSeconds()), log.getMessagesFromIndex(logIndex));

		return iDesign;
	}

	// This function is the one that will be implemented by each specific designer to implement the actual logic!
	protected abstract PhysicalDesign internalDesign(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception;
	
	// This function is supposed to return a single string that captures all the algorithmic parameters of the designer that can affect its output
	protected abstract String signature();
	
}
