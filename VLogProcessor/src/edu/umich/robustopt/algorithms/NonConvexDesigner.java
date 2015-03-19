package edu.umich.robustopt.algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.stat.StatUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryWindow;
import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.MyMathUtils;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.util.Triple;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile;
import edu.umich.robustopt.workloads.WorkloadGenerator;

public class NonConvexDesigner extends RobustDesigner {
	//private final int version = 1; // we keep moving the window!
	// private final int version = 2;  // we only find neibors around the original window
	//private final int version = 3; // use all neighbors (instead of just the worst ones) and choose the neighbors only once! (had a bug in window merging code, instead of prevFreq + alpha*badFreq we were doing something else!)
	//private final int version = 4; // like version 3 but we fixed its bug: in the window merging code, instead of prevFreq + alpha*badFreq we were doing something else!
	//private final int version = 5;
	private final int version;
	
	private final int iterationsToConverge;
	private final double initialWeight;
	private final double failureFactor;
	private final double successFactor;
	private boolean deterministic; // if this variable is true, we only look at the first query in each cluster, otherwise we pick one at random each time!
	private LatencyMeter latencyMeter;
	private boolean useExplainInsteadOfRunningQueries;
	private int maximumQueriesPerWindow;
	private final double maxFactionOfWorstSolutions; // used for choosing worst designs 
	private final double avgDistanceFactorToFormAGap; // used for choosing worst designs
	private final double noticeableRelativeDifference; // used when comparing performance of two designs
	private DistributionDistanceGenerator distGenerator;
	
	public NonConvexDesigner(int version, LogLevel verbosity, DBDesigner dbDesigner,
			DBDeployer dbDeployer, DesignParameters designMode,
			ExperimentCache experimentCache,
			WorkloadGenerator workloadGenerator, int howManyPurturbation, int iterationsToConverge, double initialWeight, 
			double successFactor, double failureFactor, boolean deterministic, 
			double maxFactionOfWorstSolutions, double avgDistanceFactorToFormAGap, double noticeableRelativeDifference,
			LatencyMeter latencyMeter, boolean useExplainInsteadOfRunningQueries, int maximumQueriesPerWindow, 
			DistributionDistanceGenerator distGenerator) {
		super(verbosity, dbDesigner, dbDeployer, designMode, experimentCache,
				workloadGenerator, howManyPurturbation);
		assert(iterationsToConverge >= 1 && initialWeight>0.5 && initialWeight<1);
		this.version = version;
		this.iterationsToConverge = iterationsToConverge;
		this.initialWeight = initialWeight;
		assert(Math.abs(failureFactor-1)>0.05);
		this.failureFactor = failureFactor;
		this.successFactor = successFactor;
		this.deterministic = deterministic;
		this.maxFactionOfWorstSolutions = maxFactionOfWorstSolutions;
		this.avgDistanceFactorToFormAGap = avgDistanceFactorToFormAGap;
		this.noticeableRelativeDifference = noticeableRelativeDifference;
		this.latencyMeter = latencyMeter;
		this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
		this.maximumQueriesPerWindow = maximumQueriesPerWindow;
		this.distGenerator = distGenerator;
	}

	
	private PhysicalDesign findDesignForClusteredWindow(ClusteredWindow clusteredWindow) throws Exception {
		List<WeightedQuery> weightedQueries = convertClusteredWindowIntoWeightedQueries(clusteredWindow);
		PhysicalDesign design = dbDesigner.findDesignByWeightedQueries(weightedQueries, designParameters);
		return design;
	}
	
	private List<WeightedQuery> convertClusteredWindowIntoWeightedQueries(ClusteredWindow clusteredWindow) throws Exception {
		List<WeightedQuery> queries = new ArrayList<WeightedQuery>();
		for (Cluster c : clusteredWindow.getClusters()) {
			Query q = (this.deterministic? c.retrieveAQueryAtPosition(0) : c.retrieveAQueryAtRandom());
			WeightedQuery wq = new WeightedQuery(q.getSql(), (double) c.getFrequency());
			queries.add(wq);
		}
		return queries;
	}

	@Override
	protected PhysicalDesign internalDesign(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {
		PhysicalDesign design;
		Timer overallT = new Timer();

		switch (version) {
			case 1:
			case 3:
				throw new Exception("This old versions are no longer supported: " + version);
				
			case 2:
				design = internalDesign_version2(pastWindowQueries, distributionDistance);
				break;
				
			case 4:
				design = internalDesign_version46(pastWindowQueries, distributionDistance);
				break;
				
			case 5:
				design = internalDesign_version5(pastWindowQueries, distributionDistance);
				break;
				
			case 6:
				design = internalDesign_version46(pastWindowQueries, distributionDistance);
				break;
				
			default:
				throw new Exception("Unknown version : " + version);
		}
		
		log.status(LogLevel.STATUS, "NonConvexDesigner overall took " + overallT.lapMinutes() + " minutes for this window");		
		return design;
	}
	
	//version 5.0
	/* Description: 1) We always keep the original window, 
	 * 				2) at each step we keep increasing the weight of ALL the neighbors of the current window until we find a design that improves on those neighbors
	 * W0: initial window
	 * 
	 * curWin = W0
	 * D = Des(curWin)
	 * P = {p1, p2, ..., pN} = perturbed windows of distance d from Wn 
	 * for iter=1, 2, ...
	 * 		Q = {q1, ..., qK} = worst queries, i.e., top K queries with the largest (fi, Li) where fi and Li are the frequency and latency of qi
	 *		W' = curWin + ((f1*L1)^alpha, q1) + ... ((fK*LK)^alpha, qK) // merge
	 *		D' = Des(W')
	 *		if L(P, D') < L(P, D)   // success
	 *			D = D'
	 * 			alpha = alpha * successFac (<1)
	 * 		else
	 * 			alpha = alpha * failureFac (>1)
	 */
	private PhysicalDesign internalDesign_version5(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {
		// we need 2 copies because we will keep changing the curWindow 		
		ClusteredWindow initialWindow = workloadGenerator.getClustering().convertSqlListIntoClusteredWindow(pastWindowQueries, workloadGenerator);	
		List<ClusteredWindow> perturbedWindows = generatePurturbedWindows(initialWindow, distributionDistance); 
//		
		for (int i=0; i<perturbedWindows.size(); ++i) {
			int count = perturbedWindows.get(i).countQueryOccurrence("select a.ident_2669, a.ident_2251, b.ident_2251, a.ident_451 from st_etl_2.ident_83 a left outer join st_etl_2.ident_83 b on a.ident_2669 = b.ident_2669 and b.ident_2251 = a.ident_2251");
			if (count>0)
				log.status(LogLevel.STATUS, "The " + i + "'th window contained your query this many times: " + count);
		}
//		
		PhysicalDesign currentDesign = findDesignForClusteredWindow(initialWindow);
		dbDeployer.deployDesign(currentDesign.getPhysicalStructuresAsList(), false);
		
		List<Pair<Cluster, Double>> worstClustersCosts = null;
		double alpha = initialWeight;
		int iter = 0; // current stopping criterion is based on a fixed count

		// DY : putting this declaration here since we need total elapsed design time to be cached by experimentCache.
		while (iter++ < iterationsToConverge) {
			Timer iterationDesignTime = new Timer(); int logIndex = log.getNextIndex();
			worstClustersCosts = (worstClustersCosts==null ? findWorstClustersCosts(currentDesign, perturbedWindows) : worstClustersCosts);
			ClusteredWindow newWindow = mergeClusteredWindowWithClusters(initialWindow, worstClustersCosts, alpha);
			PhysicalDesign newDesign = findDesignForClusteredWindow(newWindow);
			
			if (newDesign.equals(currentDesign))
				log.status(LogLevel.WARNING, "Error: The new design is identical to the old one! You need to re-consider your parameters!");
			
			log.status(LogLevel.VERBOSE, "New merged window under consideration is:\n" + newWindow.showFrequencyDistribution()); log.status(LogLevel.DEBUG, newWindow.toString());
			log.status(LogLevel.DEBUG, "Current Design is:\n" + currentDesign.toString() + "\n but new alternative is:\n" + newDesign.toString());
			dbDeployer.deployDesign(newDesign.getPhysicalStructuresAsList(), false);
						
			if (isABetterDesign(newDesign, currentDesign, perturbedWindows, true)) {
				currentDesign = newDesign;
				worstClustersCosts = null;
				alpha *= successFactor;
				DistributionDistance dist = distGenerator.distance(initialWindow.getAllQueries(), newWindow.getAllQueries());
				log.status(LogLevel.VERBOSE, "(" + signature() + ") Iteration " + iter + " we SUCCESSFULY improved the design, new alpha = " + alpha + ", and we moved to a window of distance="+dist+" from our original window" + " (took " + iterationDesignTime.lapMinutes() + " mins)");				
			} else {
				alpha *= failureFactor;
				log.status(LogLevel.VERBOSE, "(" + signature() + ") Iteration " + iter + " we could NOT improve the design, new alpha = " + alpha + " (took " + iterationDesignTime.lapMinutes() + " mins)");
			}
			
			if (experimentCache!=null)
				experimentCache.cacheDesignByQueriesAlgorithm(pastWindowQueries, getName(), robustSignature(distributionDistance, iter), currentDesign, (long)iterationDesignTime.lapSeconds(), log.getMessagesFromIndex(logIndex));		
		}
		
		return currentDesign;
	}



	//version 4.0 and version 6.0
	/* Description: 1) We always keep the original, 
	 * 				2) at each step we keep increasing the weight of ALL the neighbors of the current window until we find a design that improves on those neighbors
	 * W0: initial window
	 * 
	 * curWin = W0
	 * D = Des(curWin)
	 * P = {p1, p2, ..., pN} = perturbed windows of distance d from Wn 
	 * for iter=1, 2, ...
	 * 		PP = (P in version 4.0 and K worst windows w1,..,wK in version 6.0) 
	 *		W' = curWin + (alpha*f1, p1) + ... (alpha*fk, pk) // merge
	 *		D' = Des(W')
	 *		if L(PP, D') < L(PP, D)   // success
	 *			D = D'
	 * 			alpha = alpha * successFac (<1)
	 * 		else
	 * 			alpha = alpha * failureFac (>1)
	 */
	private PhysicalDesign internalDesign_version46(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {
		// we need 2 copies because we will keep changing the curWindow 		
		ClusteredWindow initialWindow = workloadGenerator.getClustering().convertSqlListIntoClusteredWindow(pastWindowQueries, workloadGenerator);	
		List<ClusteredWindow> perturbedWindows = generatePurturbedWindows(initialWindow, distributionDistance);
		PhysicalDesign currentDesign = findDesignForClusteredWindow(initialWindow);
		dbDeployer.deployDesign(currentDesign.getPhysicalStructuresAsList(), false);

		List<ClusteredWindow> worstPossibleWindows = null;
		double alpha = initialWeight;
		int iter = 0; // current stopping criterion is based on a fixed count
		while (iter++ < iterationsToConverge) {
			Timer iterationDesignTime = new Timer(); int logIndex = log.getNextIndex();
			if (worstPossibleWindows==null)
				worstPossibleWindows = (version==4 ? perturbedWindows : findWorstPossibleWindows(currentDesign, perturbedWindows));
			ClusteredWindow newWindow = mergeClusteredWindows(initialWindow, worstPossibleWindows, alpha);

			PhysicalDesign newDesign = findDesignForClusteredWindow(newWindow);
			if (newDesign.equals(currentDesign))
				log.status(LogLevel.WARNING, "Error: The new design is identical to the old one! You need to re-consider your parameters!");

			log.status(LogLevel.VERBOSE, "New merged window under consideration is:\n" + newWindow.showFrequencyDistribution()); log.status(LogLevel.DEBUG, newWindow.toString());
			log.status(LogLevel.DEBUG, "Current Design is:\n" + currentDesign.toString() + "\n but new alternative is:\n" + newDesign.toString());
			dbDeployer.deployDesign(newDesign.getPhysicalStructuresAsList(), false);
						
			if (isABetterDesign(newDesign, currentDesign, worstPossibleWindows, true)) {
				currentDesign = newDesign;
				worstPossibleWindows = null;
				alpha *= successFactor;
				DistributionDistance dist = distGenerator.distance(initialWindow.getAllQueries(), newWindow.getAllQueries());
				log.status(LogLevel.VERBOSE, "(" + signature() + ") Iteration " + iter + " we SUCCESSFULY improved the design, new alpha = " + alpha + ", and we moved to a window of distance="+dist+" from our original window" + " (took " + iterationDesignTime.lapMinutes() + " mins)");				
			} else {
				alpha *= failureFactor;
				log.status(LogLevel.VERBOSE, "(" + signature() + ") Iteration " + iter + " we could NOT improve the design, new alpha = " + alpha + " (took " + iterationDesignTime.lapMinutes() + " mins)");
			}
			
			if (experimentCache!=null)
				experimentCache.cacheDesignByQueriesAlgorithm(pastWindowQueries, getName(), robustSignature(distributionDistance, iter), currentDesign, (long)iterationDesignTime.lapSeconds(), log.getMessagesFromIndex(logIndex));		
		}
		
		return currentDesign;
	}

	//version 2.0: 
	/* Description: 1) We keep moving the window, 
	 * 				2) at each window we keep increasing the weight of the worst neighbors of the current window until we find a design that improves on those worst neighbors
	 * W0: initial window
	 * 
	 * curWin = W0
	 * D = Des(curWin)
	 * for iter=1, 2, ...
	 * 		{p1, p2, ..., pN} = perturbed windows of distance d from Wn 
	 * 		worstWins = {w1, w2, ..., wk} = choose worst windows from p1, ..., pN
	 *		W' = curWin + (alpha*f1, w1) + ... (alpha*fk, wk) // merge
	 *		D' = Des(W')
	 *		if L(worstWins, D') < L(worstWin, D)   // success
	 *			curWin = W'
	 *			D = D'
	 * 			alpha = alpha * successFac (<1)
	 * 		else
	 * 			alpha = alpha * failureFac (>1)
	 */
	private PhysicalDesign internalDesign_version2(List<String> pastWindowQueries, DistributionDistance distributionDistance) throws Exception {
		// we need 2 copies because we will keep changing the curWindow 		
		ClusteredWindow initialWindow = workloadGenerator.getClustering().convertSqlListIntoClusteredWindow(pastWindowQueries, workloadGenerator);
		ClusteredWindow curWindow = workloadGenerator.getClustering().convertSqlListIntoClusteredWindow(pastWindowQueries, workloadGenerator);
		PhysicalDesign currentDesign = findDesignForClusteredWindow(curWindow);
		
		List<ClusteredWindow> worstPossibleWindows = null;
		double alpha = initialWeight;
		int iter = 0; // current stopping criterion is based on a fixed count
		while (iter++ < iterationsToConverge) {
			Timer iterationDesignTime = new Timer(); int logIndex = log.getNextIndex();
			// find the worst solution within distributionDistance of the current window (it will be null if it's the first time or if the previous curWindow has changed!)
			//worstPossibleWindows = (worstPossibleWindows==null ? findWorstPossibleWindows(currentDesign, curWindow, distributionDistance) : worstPossibleWindows);
			//ClusteredWindow newWindow = mergeClusteredWindows(curWindow, worstPossibleWindows, alpha);
			worstPossibleWindows = (worstPossibleWindows==null ? findWorstPossibleWindows(currentDesign, initialWindow, distributionDistance) : worstPossibleWindows);
			ClusteredWindow newWindow = mergeClusteredWindows(initialWindow, worstPossibleWindows, alpha);
			PhysicalDesign newDesign = findDesignForClusteredWindow(newWindow);
			
			if (newDesign.equals(currentDesign))
				log.status(LogLevel.WARNING, "Error: The new design is identical to the old one! You need to re-consider your parameters!");
			
			log.status(LogLevel.VERBOSE, "Current Window is:\n" + curWindow.toString() + "\n and new window is:\n" + newWindow.toString());
			log.status(LogLevel.DEBUG, "Current Design is:\n" + currentDesign.toString() + "\n but new alternative is:\n" + newDesign.toString());
			dbDeployer.deployDesign(newDesign.getPhysicalStructuresAsList(), false);
						
			if (isABetterDesign(newDesign, currentDesign, worstPossibleWindows, true)) {
				curWindow = newWindow;
				currentDesign = newDesign;
				worstPossibleWindows = null; // so that we look for them anew!
				alpha *= successFactor;
				DistributionDistance dist = distGenerator.distance(initialWindow.getAllQueries(), curWindow.getAllQueries());
				log.status(LogLevel.STATUS, "(" + signature() + ") Iteration " + iter + " we SUCCESSFULY improved the design, new alpha = " + alpha + ", and we moved to a window of distance="+dist+" from our original window" + " (took " + iterationDesignTime.lapMinutes() + " mins)");				
			} else {
				alpha *= failureFactor;
				log.status(LogLevel.STATUS, "(" + signature() + ") Iteration " + iter + " we could NOT improve the design, new alpha = " + alpha + " (took " + iterationDesignTime.lapMinutes() + " mins)");
			}
			
			if (experimentCache!=null)
				experimentCache.cacheDesignByQueriesAlgorithm(pastWindowQueries, getName(), robustSignature(distributionDistance, iter), currentDesign, (long)iterationDesignTime.lapSeconds(), log.getMessagesFromIndex(logIndex));		
		}
		
		return currentDesign;
	}


	// we are considering two designs (design1 and design2) and we have a set of possibilities
	// we return true if the max/avg performance of design 1 for these possibilities is better than that of design2
	private boolean isABetterDesign(PhysicalDesign design1, PhysicalDesign design2, List<ClusteredWindow> possibleWindows, boolean judgeBasedOnWorstCase) throws Exception {
		List<Long> latencies1 = latencyMeter.measureAvgLatenciesForMultipleClusteredWindows(possibleWindows, design1.getPhysicalStructuresAsList(), true, useExplainInsteadOfRunningQueries);
		List<Long> latencies2 = latencyMeter.measureAvgLatenciesForMultipleClusteredWindows(possibleWindows, design2.getPhysicalStructuresAsList(), true, useExplainInsteadOfRunningQueries);
		
		long max1 = MyMathUtils.getMaxLongs(latencies1), avg1 = MyMathUtils.getMeanLongs(latencies1);
		long max2 = MyMathUtils.getMaxLongs(latencies2), avg2 = MyMathUtils.getMeanLongs(latencies2);
		
		boolean isBetter;
		
		if (judgeBasedOnWorstCase) { // use max 
			isBetter = (comparable(max1, max2) ? avg1 < avg2 : max1 < max2); 
		} else { // use avg
			isBetter = (comparable(avg1, avg2) ? max1 < max2 : avg1 < avg2); 
		}
		
		log.status(LogLevel.VERBOSE, "isABetterDesign: (max1, avg1)=("+ max1 +", " + avg1 + ") and (max2,avg2)=(" + max2 + ", " + avg2 + ") => decision: design one is " + (isBetter? "" : "NOT ") + "better!");
		return isBetter;
	}
	
	private boolean comparable(long value1, long value2) {
		int howManyInfs = (value1==Long.MAX_VALUE ? 1 : 0) + (value2==Long.MAX_VALUE ? 1 : 0);
		
		if (howManyInfs == 2)
			return true;
		else if (howManyInfs == 1)
			return false;
		// none of them are Infinities!
		double v1 = Math.abs(value1), v2 = Math.abs(value2);
		return Math.abs(v1-v2) <= noticeableRelativeDifference*Math.max(v1, v2);
	}

	// find the worst windows that are within a distance of distributionDistance from currentDesign which lead to the worst performance given the current design
	private List<ClusteredWindow> findWorstPossibleWindows(PhysicalDesign currentDesign, List<ClusteredWindow> possibleWindows) throws Exception {
		// estimate the performance cost of each neighbor using current design
		List<Number> latencies = new ArrayList<Number>();
		for (ClusteredWindow clusteredWindow : possibleWindows) {
			long neighborLatency = latencyMeter.measureAvgLatencyForOneClusteredWindow(clusteredWindow, currentDesign.getPhysicalStructuresAsList(), true, useExplainInsteadOfRunningQueries);
			latencies.add(neighborLatency);
		}
			
		// choose the top cost!
		long thresholdToBeCalledMaximum = findMaximumThreshold(latencies).longValue();
		
		// choose any neighbor with a cost >=maximumThreshold as one of the worst neighbors!
		List<ClusteredWindow> worstNeighbors = new ArrayList<ClusteredWindow>();
		for (int i=0; i<latencies.size(); ++i)
			if (latencies.get(i).longValue() >= thresholdToBeCalledMaximum)
				worstNeighbors.add(possibleWindows.get(i));
		
		log.status(LogLevel.VERBOSE, "chose " + worstNeighbors.size() + " out of " + latencies.size() + " neighbors as worst ones, i.e. those whose latency >= " + thresholdToBeCalledMaximum);
		return worstNeighbors;
	}
	
	// find the worst windows that are within a distance of distributionDistance from currentDesign which lead to the worst performance given the current design
	private List<ClusteredWindow> findWorstPossibleWindows(PhysicalDesign currentDesign, ClusteredWindow curWindow, 
			DistributionDistance distributionDistance) throws Exception {
		// let's sample the space first
		List<ClusteredWindow> someNeighboringWindows = generatePurturbedWindows(curWindow, distributionDistance);
		
		List<ClusteredWindow> worstNeighbors = findWorstPossibleWindows(currentDesign, someNeighboringWindows);
		
		return worstNeighbors;
	}

	private List<Pair<Cluster, Double>> findWorstClustersCosts(PhysicalDesign currentDesign, List<ClusteredWindow> perturbedWindows) throws Exception {
		Map<Cluster, Double> allClustersCost = new HashMap<Cluster, Double>();
		for (ClusteredWindow cWindow : perturbedWindows)
			for (Cluster c: cWindow.getClusters()) {
				List<Cluster> fakeList = Arrays.asList(c);
				long latency = latencyMeter.measureAvgLatencyForOneClusteredWindow(new ClusteredWindow(fakeList), currentDesign.getPhysicalStructuresAsList(), true, useExplainInsteadOfRunningQueries);
				double cost = (latency == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : latency * c.getFrequency()); 
				if (!allClustersCost.containsKey(c))
					allClustersCost.put(c, cost);
				else {
					double prevCost = allClustersCost.get(c);
					double newCost = (prevCost==Double.POSITIVE_INFINITY || cost == Double.POSITIVE_INFINITY ? Double.POSITIVE_INFINITY : prevCost + cost);
					allClustersCost.put(c, newCost);
				}
			}

		// choose the top cost!
		List<Number> costsN = new ArrayList<Number>();
		for (Double D : allClustersCost.values())
			costsN.add(D);
		Double thresholdToBeCalledMaximum = findMaximumThreshold(costsN).doubleValue();
		
		// choose any neighbor with a cost >=maximumThreshold as one of the worst neighbors!
		List<Pair<Cluster, Double>> worstClustersCosts = new ArrayList<Pair<Cluster,Double>>();
		for (Cluster c : allClustersCost.keySet())
			if (allClustersCost.get(c) >= thresholdToBeCalledMaximum) {
				worstClustersCosts.add(new Pair<Cluster, Double>(c, allClustersCost.get(c)));
				log.status(LogLevel.STATUS, "Chosen worst-neighbor (freq=" + c.getFrequency() + "): " + c.retrieveAQueryAtPosition(0).getSql());
			}
		
		log.status(LogLevel.VERBOSE, "chose " + worstClustersCosts.size() + " clusters out of " + allClustersCost.size() + " clusters as worst ones, i.e. those whose latency >= " + thresholdToBeCalledMaximum);
		return worstClustersCosts;
	}

	// This function computes the break point between the top values and the rest of them
	private Number findMaximumThreshold(List<Number> latencies) throws Exception {
		Number min, max, range;
		double quantile = maxFactionOfWorstSolutions;
		double quantileValue; 
		double avgDistanceMultiplier = avgDistanceFactorToFormAGap;

		if (latencies.get(0) instanceof Long) {
			List<Long> latenciesL = new ArrayList<Long>();
			for (Number N : latencies)
				latenciesL.add((Long) N);
			min = MyMathUtils.getMinLongs(latenciesL);
			
			List<Long> nonInfinityLongs = new ArrayList<Long>();
			for (Long L : latenciesL)
				if (L != Long.MAX_VALUE)
					nonInfinityLongs.add(L);
			max = MyMathUtils.getMaxLongs(nonInfinityLongs);
			quantileValue = MyMathUtils.percentile(latenciesL, quantile*100);
		} else if (latencies.get(0) instanceof Double) {
			double latenciesD[] = new double[latencies.size()];
			for (int i=0; i<latencies.size(); ++i)
				latenciesD[i] = (Double) latencies.get(i);
			quantileValue = StatUtils.percentile(latenciesD, quantile*100);
			min = StatUtils.min(latenciesD);
			for (int i=0; i<latenciesD.length; ++i)
				if (latenciesD[i] == Double.POSITIVE_INFINITY)
					latenciesD[i] = Double.NEGATIVE_INFINITY; // so that they won't show up for infinity!
			double maxD = StatUtils.max(latenciesD);
			if (maxD==Double.NEGATIVE_INFINITY)
				throw new Exception("Everything was infinity!");
			else
				max = maxD;
		} else
			throw new Exception("Type not supported.");
		
		range = max.doubleValue() - min.doubleValue();
		double avgDistance = range.doubleValue() / latencies.size();

		List<Number> sortedLatencies = new ArrayList<Number>(latencies);
		Collections.sort(sortedLatencies, Collections.reverseOrder()); 
		
		Number threshold = sortedLatencies.get(0);
		for (int i=1; i<sortedLatencies.size(); ++i)
			if (sortedLatencies.get(i).doubleValue() < quantileValue)
				break;
			else if (threshold.doubleValue() == Double.POSITIVE_INFINITY 
					|| 
					threshold.doubleValue() - sortedLatencies.get(i).doubleValue() <= avgDistanceMultiplier*avgDistance)
				threshold = sortedLatencies.get(i);
	
		StringBuilder msgBuf = new StringBuilder("findMaximumThreshold: [avgDist=" + avgDistance + ", quantile="+quantileValue+"]: ");
		boolean foundGap = false;
		for (int i=0; i<sortedLatencies.size(); ++i) {
			msgBuf.append(sortedLatencies.get(i) + ", ");
			if (!foundGap && i<sortedLatencies.size()-1 && sortedLatencies.get(i+1).doubleValue() < threshold.doubleValue()) {
				msgBuf.append(" <GAP> ");
				foundGap = true;
			}
		}
		log.status(LogLevel.VERBOSE, msgBuf.toString());
	
		return threshold;
	}

	// merge the curWindow with a set of other windows by modifying the weight (frequency) of the queries in the other windows by a factor of alpha
	private ClusteredWindow mergeClusteredWindows(ClusteredWindow curWindow, List<ClusteredWindow> worstPossibleWindows, double alpha) throws Exception {
		StringBuilder logMsg = new StringBuilder("mergeClusteredWindows: We merged this window with its worst case neighbors (alpha="+alpha+"):\n"+ curWindow.toString());
		
		Map<Cluster, Double> allClusters = new HashMap<Cluster, Double>();
		// first add all the clusters from the current window
		for (Cluster c : curWindow.getClusters())
			allClusters.put(c, (double)c.getFrequency());

		// we need this to measure the latency weights!
		PhysicalDesign curDesign = findDesignForClusteredWindow(curWindow);
		dbDeployer.deployDesign(curDesign.getPhysicalStructuresAsList(), false);
		
		// now add all clusters from the worst solutions. 
		for (ClusteredWindow worstPossibleWindow : worstPossibleWindows)
			for (Cluster c : worstPossibleWindow.getClusters()) {
				long latency = latencyMeter.measureAvgLatencyForOneClusteredWindow(new ClusteredWindow(Arrays.asList(c)), curDesign.getPhysicalStructuresAsList(), true, useExplainInsteadOfRunningQueries);
				long cost = (latency==Long.MAX_VALUE ? Long.MAX_VALUE : c.getFrequency() * latency);
				double amplifiedCost = (cost == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : Math.pow(cost, alpha));
				if (!allClusters.containsKey(c))
					allClusters.put(c, amplifiedCost);
				else {
					double prevFreq = allClusters.get(c);
					double newFreq = (prevFreq==Double.POSITIVE_INFINITY || amplifiedCost==Double.POSITIVE_INFINITY ? Double.POSITIVE_INFINITY : prevFreq + amplifiedCost);
					allClusters.put(c, newFreq);
				}
				//System.out.println("Freq=" + allClusters.get(c));
			}
		
		ClusteredWindow mergedClusteredWindow = createAValidClusteredWindow(allClusters);
		log.status(LogLevel.DEBUG, logMsg.toString() + " and got the following merged window:\n " + mergedClusteredWindow .toString());
		
		return mergedClusteredWindow;
	}
	
	private ClusteredWindow mergeClusteredWindowWithClusters(ClusteredWindow initialWindow, List<Pair<Cluster, Double>> worstClustersCosts, double alpha) throws Exception {
		StringBuilder logMsg = new StringBuilder("mergeClusteredWindowWithClusters: We merged this window with some worst case individual clusters (alpha="+alpha+"):\n"+ initialWindow.toString());

		Map<Cluster, Double> allClusters = new HashMap<Cluster, Double>();
		// first add all the clusters from the current window
		for (Cluster c : initialWindow.getClusters())
			allClusters.put(c, (double)c.getFrequency());

		// now add all clusters from the worst solutions.
		for (Pair<Cluster, Double> clusterCost : worstClustersCosts) {
			Cluster c = clusterCost.first; //TODO: what is cost? Does it already include the latency being infinite?
			Double cost = clusterCost.second;
			cost = (cost==Double.POSITIVE_INFINITY ? Double.POSITIVE_INFINITY  : Math.pow(cost, alpha));

			if (!allClusters.containsKey(c))
				allClusters.put(c, cost);
			else {
				double prevCost = allClusters.get(c);
				double newCost = (prevCost==Double.POSITIVE_INFINITY || cost==Double.POSITIVE_INFINITY ? Double.POSITIVE_INFINITY : prevCost + cost);
				allClusters.put(c, newCost);
			}
			//System.out.println("Freq=" + allClusters.get(c));
		}
		
		ClusteredWindow mergedClusteredWindow = createAValidClusteredWindow(allClusters);
		log.status(LogLevel.DEBUG, logMsg.toString() + " and got the following merged window:\n " + mergedClusteredWindow .toString());

		return mergedClusteredWindow;
	}

	private ClusteredWindow createAValidClusteredWindow(Map<Cluster, Double> allClusters) throws Exception {
		int howManyInfinity = 0;
		for (Cluster c : allClusters.keySet())
			if (allClusters.get(c)==Double.POSITIVE_INFINITY)
				++howManyInfinity;
		
		Set<Cluster> mergedWindow = new HashSet<Cluster>();
		
		if (howManyInfinity >= 1) {
			int budget = maximumQueriesPerWindow;
			int freq = (int) Math.round(budget / (double)howManyInfinity);
			if (freq == 0)
				freq = 1;
			
			for (Cluster c : allClusters.keySet())
				if (allClusters.get(c)==Double.POSITIVE_INFINITY) // the others which are not infinity will be ignored (set to zero!)
					if (budget>=freq) {
						Cluster newCluster = workloadGenerator.createClusterWithNewFrequency(c, freq);
						mergedWindow.add(newCluster);
						budget -= freq;
					}
					
		} else { // we don't have any infinity!
			double totalFreq = 0.0;
			for (Cluster c : allClusters.keySet())
				totalFreq += allClusters.get(c);
			double normalizingRatio = (totalFreq <= maximumQueriesPerWindow ? 1.0 : maximumQueriesPerWindow/ totalFreq);
					
			for (Cluster c : allClusters.keySet()) {
				int freq = (int) Math.round(allClusters.get(c) * normalizingRatio);
				if (freq >= 1) { // otherwise, we do not want to create empty clusters!
					Cluster newCluster = workloadGenerator.createClusterWithNewFrequency(c, freq);
					mergedWindow.add(newCluster);
				}
			}
		} // end of no-infinity case
		
		ClusteredWindow validWindow = new ClusteredWindow(mergedWindow);
		
		return validWindow;
	}
	
	@Override
	protected String signature() {
		String signature =  signature(iterationsToConverge);
		
		return signature;
	}

	public String summarizeParameters() {
		return signature();
	}
	
	protected String signature(int iteration) {
		String signature = "designMode=" + designParameters + ", purtubs=" + howManyPurturbation 
				+ ", iterationsToConverge=" + iteration
				+", initialWeight=" + initialWeight +", failureFactor=" + failureFactor +", successFactor=" + successFactor 
				+", deterministic=" + deterministic +", maxFactionOfWorstSolutions=" + maxFactionOfWorstSolutions +", avgDistanceFactorToFormAGap=" + avgDistanceFactorToFormAGap 
				+", noticeableRelativeDifference=" + noticeableRelativeDifference   
				+", useExplainInsteadOfRunningQueries=" + useExplainInsteadOfRunningQueries 
				+", maximumQueriesPerWindow=" + maximumQueriesPerWindow
				+ (version==1? "" : ", version=" + version);
		
		return signature;
	}

	protected String robustSignature(DistributionDistance distributionDistance, int iteration) {
		String sign = "distributionDistance=" + distributionDistance + ", " + signature(iteration);
		return sign;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		NonConvexDesigner nonConvexDesigner = loadNonConvexDesignerFromFile("/Users/sina/robust-opt/VLogProcessor/data/cliffguard.conf", "default", null, null, null, null);
	}

	public static NonConvexDesigner loadNonConvexDesignerFromFile(String configFile, String beanName, DBDesigner dbDesigner, DBDeployer dbDeployer, LatencyMeter latencyMeter, ExperimentCache experimentCache) {
        BeanFactory factory = new XmlBeanFactory(new FileSystemResource(configFile));
        NonConvexDesigner nonConvexDesigner = (NonConvexDesigner) factory.getBean(beanName);
		nonConvexDesigner.dbDesigner = dbDesigner;
		nonConvexDesigner.dbDeployer = dbDeployer;
		nonConvexDesigner.latencyMeter = latencyMeter;
		nonConvexDesigner.experimentCache = experimentCache;		
		if (nonConvexDesigner.workloadGenerator instanceof EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile) {
			EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile workloadGen = (EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile) nonConvexDesigner.workloadGenerator;
			workloadGen.setLatencyMeter(latencyMeter);
		}
        
		return nonConvexDesigner;
	}

}
