package edu.umich.robustopt.algorithms;

import java.sql.SQLException;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryWindow;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistancePair;
import edu.umich.robustopt.workloads.EuclideanDistance;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong;
import edu.umich.robustopt.workloads.WorkloadGenerator;

public abstract class RobustDesigner extends RealisticDesignAlgorithm {
	protected WorkloadGenerator workloadGenerator;
	protected int howManyPurturbation;

	public RobustDesigner(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer,
			DesignParameters designMode, ExperimentCache experimentCache, WorkloadGenerator workloadGenerator, int howManyPurturbation) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache);
		this.workloadGenerator = workloadGenerator;
		this.howManyPurturbation = howManyPurturbation;
	}

	// creates howManyPurturbation new windows that are each distributionDistance apart from the originalWindowSql
	protected List<List<String>> generatePurturbedWindows(List<String> originalWindowSql, DistributionDistance distributionDistance) throws Exception{
		List<Query> originalWindowQueries = workloadGenerator.getQueryParser().convertSqlListToQuery(originalWindowSql, workloadGenerator.getSchemaMap());
		QueryWindow originalQueryWindow = new QueryWindow(originalWindowQueries);
		ClusteredWindow originalWindowClustered = workloadGenerator.getClustering().cluster(originalQueryWindow);
		
		List<ClusteredWindow> purturbedWindowsClustered = generatePurturbedWindows(originalWindowClustered, distributionDistance);
		
		List<List<String>> purturbedWindows = new ArrayList<List<String>>();
		for (int i=0; i<purturbedWindowsClustered.size(); ++i)
			purturbedWindows.add(purturbedWindowsClustered.get(i).getAllSql());
		
		return purturbedWindows;
	}

	// creates howManyPurturbation new windows that are each distributionDistance apart from the originalWindowSql
	protected List<ClusteredWindow> generatePurturbedWindows(ClusteredWindow originalWindowClustered, DistributionDistance distributionDistance) throws Exception{
		return generatePurturbedWindows(workloadGenerator, howManyPurturbation, log, originalWindowClustered, distributionDistance);
	}

	// creates howManyPurturbation new windows that are each distributionDistance apart from the originalWindowSql
	public static List<ClusteredWindow> generatePurturbedWindows(WorkloadGenerator workloadGenerator, int howManyPurturbation, BLog log, ClusteredWindow originalWindowClustered, DistributionDistance distributionDistance) throws Exception{
		List<ClusteredWindow> perturbedWindows = new ArrayList<ClusteredWindow>();
		
		log.status(LogLevel.DEBUG, "Computing perturbed windows ...");
		Timer t = new Timer();
		
		for (int purturbation_id = 0; purturbation_id<howManyPurturbation; ++purturbation_id) {
			ClusteredWindow purturbed_window;
			if (purturbation_id==0) {
				purturbed_window = originalWindowClustered; //TODO: Is this a good thing that we treat the original window as one of the perturbations, even though its distance is zero in this case???
			} else {
				//generate a new window
				purturbed_window = workloadGenerator.forecastNextWindow(originalWindowClustered, distributionDistance);
				if (purturbed_window == null || purturbed_window.getAllQueries().isEmpty())
					log.error("generatePurturbedWindows: forecastNextWindow returned null or empty window for diatance " + distributionDistance + " and window: " + originalWindowClustered + (purturbed_window==null));
			}										
			perturbedWindows.add(purturbed_window);
		}
		
		StringBuilder strBuf = new StringBuilder("generatePurturbedWindows: distance=" + distributionDistance + " originalWindow was:\n" + originalWindowClustered.toString() + "\nPerturbedWindows:\n");
		for (int i=0; i<perturbedWindows.size(); ++i)
			strBuf.append(i + "'th perturbed window=\n" + perturbedWindows.get(i).toString() + "\n");
		log.status(LogLevel.DEBUG, strBuf.toString());
		log.status(LogLevel.VERBOSE, "generatePurturbedWindows: took " + t.lapSeconds() + " seconds");
		
		// sanity check starts
		Set<ClusteredWindow> deduplicatedWindows = new HashSet<ClusteredWindow>(perturbedWindows);
		if (deduplicatedWindows.size() < perturbedWindows.size())
			log.error("Out of " + perturbedWindows.size() + " generated windows, there were only " + deduplicatedWindows.size() + " unique windows.");
		DistributionDistanceGenerator<EuclideanDistance> distGen = new EuclideanDistanceWithSimpleUnion.Generator(workloadGenerator.getSchemaMap(), 1.0, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		List<DistributionDistance> allDistances = new ArrayList<DistributionDistance>();
		for (ClusteredWindow win : perturbedWindows)
			allDistances.add(distGen.distance(originalWindowClustered.getAllQueries(), win.getAllQueries()));
		log.status(LogLevel.VERBOSE, "generatePurturbedWindows requested a distance of " + distributionDistance + " and we generated: " + allDistances);
		// sanity check ends
		
		return perturbedWindows;
	}

	
	protected List<PhysicalDesign> generateNominalDesignsForPurturbedWindows(List<List<String>> purturbedWindows) throws Exception {
		List<PhysicalDesign> purturbedDesigns = new ArrayList<PhysicalDesign>();
		
		for (int purturbation_id = 0; purturbation_id<purturbedWindows.size(); ++purturbation_id) {
			List<String> purturbed_window = purturbedWindows.get(purturbation_id);		
			
			log.status(LogLevel.VERBOSE, "Designing for the purtubed queries from purturb_id=" + purturbation_id + " ...\n");
			Timer t = new Timer();					
			PhysicalDesign purturbedDeisgn = dbDesigner.findDesignWithoutWeight(purturbed_window, designParameters);
			if (purturbedDeisgn == null)
				throw new RuntimeException("no projections found for purturb = " + purturbation_id + ")");
			log.status(LogLevel.VERBOSE, "Designed " + purturbedDeisgn.getPhysicalStructuresAsList().size() + " projections for purturb = " + purturbation_id + " in " + t.lapMinutes() + " minutes.\n");
			log.status(LogLevel.DEBUG, "The designed projections for purturb = " + purturbation_id + " in " + t.lapMinutes() + " minutes.\n");
			for (int view=0; view<purturbedDeisgn.getPhysicalStructuresAsList().size(); ++view)
				log.status(LogLevel.DEBUG, "The " + view + "'th projection for purturbation_id="+ purturbation_id +": "+purturbedDeisgn.getPhysicalStructuresAsList().get(view).getHumanReadableSummary());
		
			purturbedDesigns.add(purturbedDeisgn);
		}
		
		return purturbedDesigns;
	}

	protected PhysicalDesign generateNominalDesign(List<String> inputWindow) throws Exception {		
		log.status(LogLevel.VERBOSE, "Finding nominal design for a given window ...\n");
		Timer t = new Timer();					
		PhysicalDesign design = dbDesigner.findDesignWithoutWeight(inputWindow, designParameters);
		if (design == null)
			throw new RuntimeException("no projections found for the given window.");
		log.status(LogLevel.VERBOSE, "Designed " + design.size() + " projections in " + t.lapMinutes() + " minutes.\n");
		for (int view=0; view<design.size(); ++view)
			log.status(LogLevel.DEBUG, "The " + view + "'th projection: "+design.getPhysicalStructuresAsList().get(view).getHumanReadableSummary());
			
		return design;
	}

	
	protected void deployPurturbedDesigns(List<PhysicalDesign> purturbedDesigns) throws Exception {		
		for (int purturbation_id = 0; purturbation_id<purturbedDesigns.size(); ++purturbation_id) {
			PhysicalDesign purturbed_design = purturbedDesigns.get(purturbation_id);		
			
			log.status(LogLevel.VERBOSE, "deploying the design for purturb_id=" + purturbation_id + " ...\n");
			Timer t = new Timer();					
			int new_built = dbDeployer.deployDesign(purturbed_design.getPhysicalStructuresAsList(), false);
			Double minutesLapsed = t.lapMinutes();
			Double designDiskGB = dbDeployer.retrieveDesignDiskSizeInGigabytes(purturbed_design.getPhysicalStructuresAsList());
			log.status(LogLevel.VERBOSE, "Depolyed design for p_id = "+ purturbation_id + ", with " + purturbed_design.size() + " projs, out of which " + new_built + 
					" projs were built from scratch, and took "+ minutesLapsed + " minutes. Total disk="+ designDiskGB+ " GB.\n");
		}
	}
	

	protected void deployDesign(List<PhysicalStructure> design) throws Exception {		
		log.status(LogLevel.VERBOSE, "deploying a given design ...\n");
		Timer t = new Timer();					
		int new_built = dbDeployer.deployDesign(design, false);
		Double minutesLapsed = t.lapMinutes();
		Double designDiskGB = dbDeployer.retrieveDesignDiskSizeInGigabytes(design);
		log.status(LogLevel.VERBOSE, "Depolyed design with " + design.size() + " projs, out of which " + new_built + 
				" projs were built from scratch, and took "+ minutesLapsed + " minutes. Total disk="+ designDiskGB+ " GB.\n");
	}
	
	protected List<PhysicalDesign> generateWeightedDesignsForPurturbedWindows(List<List<WeightedQuery>> purturbedWindowsWeightedQueries) throws Exception {
		List<PhysicalDesign> purturbedDesigns = new ArrayList<PhysicalDesign>();
		
		for (int purturbation_id = 0; purturbation_id<purturbedWindowsWeightedQueries.size(); ++purturbation_id) {
			List<WeightedQuery> weighted_window = purturbedWindowsWeightedQueries.get(purturbation_id);		
			
			log.status(LogLevel.VERBOSE, "designing for the purtubed queries from purturb_id=" + purturbation_id + " ...\n");
			Timer t = new Timer();					
			PhysicalDesign purturbedDeisgn = dbDesigner.findDesignByWeightedQueries(weighted_window, designParameters);
			if (purturbedDeisgn == null)
				throw new RuntimeException("no projections found for purturb = " + purturbation_id + ")");
			log.status(LogLevel.VERBOSE, "Designed " + purturbedDeisgn.size() + " projections for purturb = " + purturbation_id + ") in " + t.lapMinutes() + " minutes.\n");
			for (int view=0; view<purturbedDeisgn.size(); ++view)
				log.status(LogLevel.DEBUG, "proj_id=" + view + ": "+purturbedDeisgn.getPhysicalStructuresAsList().get(view).getHumanReadableSummary());
		
			purturbedDesigns.add(purturbedDeisgn);
		}
		
		return purturbedDesigns;
	}

	protected String showWindowDistances(List<String> pastWindow, List<String> curWindow, List<List<String>> perturbedWindows) throws Exception {
		StringBuilder msg = new StringBuilder();
		List<Query> pastWindowQueries = Query.convertToListOfQuery(new Query_SWGO.QParser().convertSqlListToQuery(pastWindow, workloadGenerator.getSchemaMap()));
		List<Query> curWindowQueries = Query.convertToListOfQuery(new Query_SWGO.QParser().convertSqlListToQuery(curWindow, workloadGenerator.getSchemaMap()));
		DistributionDistancePair distanceFromPastWin = new DistributionDistancePair.Generator().distance(pastWindowQueries, curWindowQueries);		
		msg.append("distance of past window to curWindow = "+distanceFromPastWin.showSummary() + "\n");

		//compute distance of diff windows to cur window
		List<String> purturbedDistanceSummaries = new ArrayList<String>(); purturbedDistanceSummaries.add("empty");
		DistributionDistancePair avgDistOfPurturbedToCurWindow = null;
		for (int w=1; w<perturbedWindows.size(); ++w) {
			List<Query> purtubedWin = Query.convertToListOfQuery(new Query_SWGO.QParser().convertSqlListToQuery(perturbedWindows.get(w), workloadGenerator.getSchemaMap()));
			DistributionDistancePair dist = new DistributionDistancePair.Generator().distance(purtubedWin, curWindowQueries);	
			msg.append("purturbed window p="+w+" has distance of " + dist.showSummary() + " to cur window\n");
			if (avgDistOfPurturbedToCurWindow == null)
				avgDistOfPurturbedToCurWindow = dist;
			else
				avgDistOfPurturbedToCurWindow = (DistributionDistancePair) avgDistOfPurturbedToCurWindow.computeAverage(avgDistOfPurturbedToCurWindow, dist); 
			purturbedDistanceSummaries.add(dist.showSummary());
		}
		msg.append("avg distance of purturbed windows to curWindow = "+avgDistOfPurturbedToCurWindow.showSummary() + "\n");
		
		return msg.toString();
	}
	
	protected String robustSignature(DistributionDistance distributionDistance) {
		String sign = "distributionDistance=" + distributionDistance + ", " + signature();
		return sign;
	}
	
	public WorkloadGenerator getWorkloadGenerator() {
		return workloadGenerator;
	}

	public static String replaceDistibutionDistanceFromSignature(String originalSignature) {
		int startIdx = originalSignature.indexOf("distributionDistance=");
		if (startIdx == -1) // could not find any distribution distance in the signature!
			return originalSignature;
		int endIdx = originalSignature.substring(startIdx).indexOf(",");
		String newString = originalSignature.substring(0, startIdx) + "distributionDistance=*" + originalSignature.substring(startIdx + endIdx);
		return newString;
	}

	
}
