package edu.umich.robustopt.algorithms;

import java.util.List;

import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.workloads.DistributionDistance;

/*
 * The WeightedExistingNominalDesigner class invokes the existing DBMS designer but uses weights for each query, i.e. it uses the DBD's API for weighted queries.
 * The query weights are derived in one of the two ways:
 * 1) if (latencyGeneratingAlgorithm!=null && latencyMeter==null)
 * 		the weight is the latency of the query stored in the PerformanceRecord under the name `latencyGeneratingAlgorithm'
 * 2) if (latencyGeneratingAlgorithm==null && latencyMeter!=null)
 * 		we measure the latency of the query while allowing no projections (i.e., an empty design) and use that latency as the query's weight
 */
public class WeightedExistingNominalDesigner extends ExistingNominalDesigner implements RequiresPerformanceRecords {
	private LatencyMeter latencyMeter;
	private Boolean useExplainInsteadOfRunningQueries;
	private String latencyGeneratingAlgorithm;

	public WeightedExistingNominalDesigner(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designMode, ExperimentCache experimentCache, LatencyMeter latencyMeter, Boolean useExplainInsteadOfRunningQueries) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		this.latencyMeter = latencyMeter;
		this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
		this.latencyGeneratingAlgorithm = null;
	}

	public WeightedExistingNominalDesigner(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designMode, ExperimentCache experimentCache, Boolean useExplainInsteadOfRunningQueries, String latencyGeneratingAlgorithm) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		this.latencyMeter = null;
		this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
		this.latencyGeneratingAlgorithm = latencyGeneratingAlgorithm;
	}
	
	@Override
	public PhysicalDesign designUsingPerformanceRecords(List<PerformanceRecord> pastWindowPerformanceRecord, DistributionDistance distributionDistance) throws Exception {
		List<WeightedQuery> weightedQueries = WeightedQuery.populateWeightsUsingLatencies(pastWindowPerformanceRecord, latencyGeneratingAlgorithm, latencyMeter, useExplainInsteadOfRunningQueries);
		PhysicalDesign nominalDesign = dbDesigner.findDesignByWeightedQueries(weightedQueries, designParameters);
		return nominalDesign;		
	}
	
	@Override
	protected String signature() {
		return "latencyMeter=" + (latencyMeter==null? "NONE" : "measured") +  ", useExplainInsteadOfRunningQueries=" + useExplainInsteadOfRunningQueries 
				+", latencyGeneratingAlgorithm=" + (latencyGeneratingAlgorithm==null? "NONE" : latencyGeneratingAlgorithm); 
	}


}
