package edu.umich.robustopt.experiments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.algorithms.DesignAlgorithm;
import edu.umich.robustopt.algorithms.ExistingNominalDesigner;
import edu.umich.robustopt.algorithms.FutureKnowingNominalDesignAlgorithm;
import edu.umich.robustopt.algorithms.NoDesigner;
import edu.umich.robustopt.algorithms.NonConvexDesigner;
import edu.umich.robustopt.algorithms.RobustDesigner;
import edu.umich.robustopt.clustering.QueryParser;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.DesignExecutionTrace;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.microsoft.MicrosoftDeployer;
import edu.umich.robustopt.microsoft.MicrosoftDesigner;
import edu.umich.robustopt.microsoft.MicrosoftLatencyMeter;
import edu.umich.robustopt.microsoft.MicrosoftQueryPlanParser;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.util.Triple;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaDeployer;
import edu.umich.robustopt.vertica.VerticaDesigner;
import edu.umich.robustopt.vertica.VerticaLatencyMeter;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistancePair;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSeparateClauses;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnionAndLatency;
import edu.umich.robustopt.workloads.EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong;


public class CliffGuard {
	
	static BLog log = new BLog(LogLevel.VERBOSE);
		
	
	private static String performDesign(DatabaseLoginConfiguration emptyDB, DatabaseLoginConfiguration fullDB, 
			String cliffGuard_config_file, String cliffGuard_setting_id,
			Double distanceValue, 
			List<String> windowQueries, String localPathToStatsFile, 
			String cacheFilename, String outputDesignScript, Boolean shouldDeploy, String outputDeploymentScript) throws Exception {				
		String dbName;
		if (!emptyDB.getDBname().equals(fullDB.getDBname())) {
			throw new Exception("The two given servers are hosting different databases:" + emptyDB.getDBname() + " versus " + fullDB.getDBname());
		} else
			dbName = emptyDB.getDBname();
		
		// we do not want to re-write the whole cache after each latency meter!
		ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile(cacheFilename, 100, 1); 

		DBDeployer dbDeployer;
		DBDesigner dbDesigner;
		LatencyMeter latencyMeter;
		if (emptyDB instanceof VerticaDatabaseLoginConfiguration) {
			if (experimentCache==null)
				experimentCache = new ExperimentCache(cacheFilename, 100, 1, 1, new VerticaQueryPlanParser());

			dbDeployer = new VerticaDeployer(LogLevel.STATUS, fullDB, experimentCache, false);
			//dbDeployer.dropAllProjections();
			//dbDeployer.copyStatistics(emptyDB, localPathToStatsFile);
			dbDesigner = new VerticaDesigner(LogLevel.STATUS, emptyDB, localPathToStatsFile, experimentCache);
			latencyMeter = new VerticaLatencyMeter(LogLevel.VERBOSE, true, fullDB, experimentCache, dbDeployer, dbDeployer, 10*60);
		} else if (emptyDB instanceof MicrosoftDatabaseLoginConfiguration) {
			if (experimentCache==null)
				experimentCache = new ExperimentCache(cacheFilename, 100, 1, 1, new MicrosoftQueryPlanParser());

			// DY: fullDB and emptyDB in SQL Server must have different names. (shell DB is using the fullDB's name)
			dbDeployer = new MicrosoftDeployer(LogLevel.STATUS, fullDB, experimentCache, false);
			//dbDeployer.dropAllProjections();
			//dbDeployer.copyStatistics(emptyDB, localPathToStatsFile);
			dbDesigner = new MicrosoftDesigner(LogLevel.STATUS, emptyDB, null, experimentCache);
			latencyMeter = new MicrosoftLatencyMeter(LogLevel.VERBOSE, true, (MicrosoftDatabaseLoginConfiguration)fullDB, experimentCache, dbDeployer, dbDeployer, 10*60);
		} else {
			throw new Exception ("Unsupported DB vendor: " + emptyDB.getClass().getSimpleName());
		}
		

		List<PerformanceRecord> windowQueryPerformance = new ArrayList<PerformanceRecord>();
		for (int q=0; q<windowQueries.size(); ++q) {
			PerformanceRecord peformanceRecord = new PerformanceRecord(windowQueries.get(q));
			windowQueryPerformance.add(peformanceRecord);
		}
		
		NonConvexDesigner nonConvexDesigner = NonConvexDesigner.loadNonConvexDesignerFromFile(cliffGuard_config_file, cliffGuard_setting_id, dbDesigner, dbDeployer, latencyMeter, experimentCache);
		
		DistributionDistanceGenerator distGen = nonConvexDesigner.getWorkloadGenerator().getDistributionDistanceGenerator();
		DistributionDistance distributionDistance;
		if (distGen instanceof EuclideanDistanceWithSimpleUnionAndLatency.Generator) {
			EuclideanDistanceWithSimpleUnionAndLatency.Generator edUL = (EuclideanDistanceWithSimpleUnionAndLatency.Generator) distGen;
			distributionDistance = new EuclideanDistanceWithSimpleUnionAndLatency(distanceValue, edUL.getPenaltyForGoingFromZeroToNonZero(), edUL.getLatencyPenaltyFactor(), edUL.getWhichClauses());
		} else if (distGen instanceof EuclideanDistanceWithSimpleUnion.Generator) {
			EuclideanDistanceWithSimpleUnion.Generator edU = (EuclideanDistanceWithSimpleUnion.Generator) distGen;
			distributionDistance = new EuclideanDistanceWithSimpleUnion(distanceValue, edU.getPenaltyForGoingFromZeroToNonZero(), edU.getWhichClauses());
		} else if (distGen instanceof EuclideanDistanceWithSeparateClauses.Generator) {
			EuclideanDistanceWithSeparateClauses edS = (EuclideanDistanceWithSeparateClauses) distGen;
			distributionDistance = new EuclideanDistanceWithSeparateClauses(distanceValue, edS.getPenaltyForGoingFromZeroToNonZero());
		} else 
			throw new Exception ("The following distance generation mode is not currently supported in this tool: " + distGen.getClass().getSimpleName());
		
		
		/*		
		double initialWeight = 1.0, successFactor = 0.5, failureFactor = 3.2;
		int numberOfIterations = 3, maximumQueriesPerWindow = 200;
		double maxFractionOfWorstSolutions = 0.3;
		double avgDistanceFactorToFormAGap = 3.0;
		double noticeableRelativeDifference = 0.01;
		
		//This is the best nonConvex Config
		maximumQueriesPerWindow = 300;
		numberOfIterations = 8;
		initialWeight = 0.03;
		failureFactor = 2;
		
		//This is just to make nonConvex faster
		numberOfIterations = 5;
		NonConvexDesigner nonConvexDesigner;

		int version = 5;
		nonConvexDesigner = new NonConvexDesigner(version, LogLevel.VERBOSE, dbDesigner, dbDeployer, designParams, experimentCache, workloadGen, howManyPurturbation, numberOfIterations, 
				initialWeight, successFactor, failureFactor, true, 
				maxFractionOfWorstSolutions, avgDistanceFactorToFormAGap, noticeableRelativeDifference, latencyMeter, useExplainInsteadOfRunningQueriesForDesign, maximumQueriesPerWindow,
				new DistributionDistancePair.Generator());
		
		*/
		
		Timer t = new Timer();
		PhysicalDesign design = nonConvexDesigner.design(windowQueryPerformance, distributionDistance);
		
		// generate script files and clean up unneeded structures
		design.generateSuggestedDesignScript(outputDesignScript);
		if (shouldDeploy) {
			dbDeployer.dropAllStructuresExcept(design.getPhysicalStructures());
		}
		else {
			dbDeployer.dropAllStructures();
			design.generateDeploymentScript(outputDeploymentScript);
		}
		
		// print summary
		log.status(LogLevel.STATUS, "finished designing in " + t.lapMinutes() + " minutes, using the following parameters for CliffGuard and distance=" + distanceValue + " : \n" + nonConvexDesigner.summarizeParameters());
		experimentCache.saveTheEntireCache(); 
		dbDesigner.closeConnection();
		dbDeployer.closeConnection();
		printStatistics(dbDesigner, dbDeployer, latencyMeter, experimentCache);
		log.status(LogLevel.STATUS, "Finished the design. The Suggested Design Script is stored in " + outputDesignScript  +  (shouldDeploy ? "" : ". The Deployment Script is stored in "
				+ outputDeploymentScript) + "\n============================================================\n\n");
		return experimentCache.getNewFileName();
	}	

	private static void printStatistics(DBDesigner dbDesigner, DBDeployer dbDeployer, LatencyMeter latencyMeter, ExperimentCache experimentCache) {
		log.status(LogLevel.STATUS, "We have the following statistics:\n============================================================\n\n");
		if (dbDesigner != null)
			log.status(LogLevel.STATUS, "**** DBDesigner: ******* \n" + dbDesigner.reportStatistics());
		if (dbDeployer != null)
			log.status(LogLevel.STATUS, "**** DBDeployer: ******* \n" + dbDeployer.reportStatistics());
		if (latencyMeter != null)
			log.status(LogLevel.STATUS, "**** LatencyMeter: ******* \n" + latencyMeter.reportStatistics());
		if (experimentCache != null)
			log.status(LogLevel.STATUS, "**** ExperimentCache: ******* \n" + experimentCache.reportStatistics());
	}

	private static void printAllDiskSizesFromCache(String serializedPlainEvaluationAlgorithmFilename, ExperimentCache cache) throws Exception {
		PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(serializedPlainEvaluationAlgorithmFilename);
		
		Map<String, PhysicalStructure> nameToProjectionMap = cache.getDeployedPhysicalStructureBaseNamesToStructures();
		Map<String, DeployedPhysicalStructure> structureToProjectionMap = cache.getDeployedPhysicalStructureNamesToDeployedPhysicalStructures();

		for (String algorithmName : plainAlgEval.getAlgorithmsWindowsDesignsDisk().keySet())
			for (int w=0; w < plainAlgEval.getAlgorithmsWindowsDesignsDisk().get(algorithmName).size(); ++w) {
				if (plainAlgEval.getAlgorithmsWindowsDesignsDisk().get(algorithmName).get(w) == null) {
					continue; // an empty window
				}
				StringBuilder msg = new StringBuilder();
				msg.append("(w=,"+w+","+ algorithmName+ "): " + plainAlgEval.getAlgorithmsWindowsDesignsDisk().get(algorithmName).get(w) + " GB\n");
				log.status(LogLevel.VERBOSE, msg.toString());
				
			}
	}

	private static void compareMultipleAlgorithmsAgainstBaseline(List<Integer> whichWindows, String evaluationFileName, ExperimentCache experimentCache, String algorithmPrefixName, String baselineName, boolean ignoreDistributionDistance) throws Exception {
		Set<String> allAlgorithmSignatures = extractAllAlgorithmSignaturesFromEvaluationFile(evaluationFileName, experimentCache, ignoreDistributionDistance);
		for (String algName : allAlgorithmSignatures)
			if (algName.startsWith(algorithmPrefixName)) {
				printComparativePerformaneFromEvaluation(whichWindows, evaluationFileName, experimentCache, baselineName, algName, ignoreDistributionDistance);
				log.status(LogLevel.STATUS, "\n\n");
			}
		log.status(LogLevel.STATUS, "\n\n");
	}
		
	private static Set<String> extractAllAlgorithmSignaturesFromEvaluationFile(String evaluationFileName, ExperimentCache experimentCache, boolean ignoreDistributionDistance) throws Exception {
		Set<String> allAlgorithmSignatures = new HashSet<String>();
		log.status(LogLevel.STATUS, "Extracting all algorithm signatures from " + evaluationFileName + " using the following cache file: " + experimentCache);
		
		PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);
		
		List<List<PerformanceRecord>> windowsQueriesPerformance = plainAlgEval.getFinalWindowsQueryPerformance();
		
		for (int w=0; w <windowsQueriesPerformance.size(); ++w) { // for each window
			for (int q=0; q < windowsQueriesPerformance.get(w).size(); ++q) { // for each query
				PerformanceRecord performanceRecord = windowsQueriesPerformance.get(w).get(q);
				Set<String> newAlgNames = performanceRecord.getAllDesignAlgorithmNames();
				
				if (ignoreDistributionDistance) {
					Set<String> tempList = new HashSet<String>();
					for (String algName : newAlgNames) 
						tempList.add(RobustDesigner.replaceDistibutionDistanceFromSignature(algName));
					newAlgNames = tempList;
				}
				
				if (!allAlgorithmSignatures.isEmpty() && !newAlgNames.equals(allAlgorithmSignatures)) {
					Set<String> diffOld = new HashSet<String>(allAlgorithmSignatures);
					Set<String> diffNew = new HashSet<String>(newAlgNames);
					diffOld.removeAll(newAlgNames);
					diffNew.removeAll(allAlgorithmSignatures);
					throw new Exception("Previous keys contained: " + diffOld + ", while the new keys contain: " + diffNew);						
				}
				
				allAlgorithmSignatures.addAll(newAlgNames);
			}
		}		

		return allAlgorithmSignatures;
	}
	
	public static String findActualSignatureBasedOnDistanceLessSignature(Set<String> actualSignatures, String distanceLessDistance) throws Exception {
		Set<String> answers = new HashSet<String>();
		String lastAnswer = null;
		for (String actualSignature : actualSignatures) 
			if (RobustDesigner.replaceDistibutionDistanceFromSignature(actualSignature).equals(distanceLessDistance)) {
				answers.add(actualSignature);
				lastAnswer = actualSignature;
			}
		
		if (answers.isEmpty())
			throw new Exception("Could not find <" + distanceLessDistance + "> in " + actualSignatures);
		
		if (answers.size()>1) 
			throw new Exception("There were multiple matches for <" + distanceLessDistance + "> in " + actualSignatures);
		
		return lastAnswer;
	}

	private static void printComparativePerformaneFromEvaluation(List<Integer> whichWindows, String evaluationFileName, ExperimentCache experimentCache, String rawBaseline, String rawOther, boolean ignoreDistributionDistance) {
		log.status(LogLevel.STATUS, "Comparing (" + rawBaseline + ") to (" + rawOther +")");
		try {			
			PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);
			
			List<List<PerformanceRecord>> windowsQueriesPerformance = plainAlgEval.getFinalWindowsQueryPerformance();

			Map<String, DeployedPhysicalStructure> nameToProjectionMap = experimentCache.getDeployedPhysicalStructureNamesToDeployedPhysicalStructures();
			Map<PhysicalStructure, String> structureToProjectionMap = experimentCache.getPhysicalStructureToDeployedNames();
			
			double ratioSum = 0;
			double baselineLatencySum = 0;
			double otherLatencySum = 0;
			int totalNeverInfiniteQueries = 0;
			
			double baselineSumAvgLatencyPerWindow = 0;
			double otherSumAvgLatencyPerWindow = 0;
			
			double baselineWorstAvgLatencyPerWindow = -1;
			double otherWorstAvgLatencyPerWindow = -1;
			
			int maxNumberOfInfBaselineToFiniteOtherPerWindow = -1;
			int maxNumberOfFiniteBaseToInfOtherlinePerWindow = -1;
			int maxNumberOfInfToInfPerWindow = -1;
			int sumNumberOfInfBaselineToFiniteOtherAllWindows = 0;
			int sumNumberOfFiniteBaseToInfOtherlineAllWindows = 0;
			int sumNumberOfInfToInfAllWindows = 0;
			int sumNumberOfFiniteToFiniteAllWindows = 0;
			
			if (whichWindows == null) {
				whichWindows = new ArrayList<Integer>();
				for (int w=0; w <windowsQueriesPerformance.size(); ++w)
					whichWindows.add(w);
			} else {
				for (Integer w : whichWindows)
					if (w <0 || w >= windowsQueriesPerformance.size())
						throw new Exception("Invalid window index: " + w +" must be between 0 and " + windowsQueriesPerformance.size());
			}
					
			for (Integer w : whichWindows) { // for each window
				int emptyQueries = 0, fullQueries = 0;
				double baselineTotalWindowLatency = 0, otherTotalWindowLatency = 0;
				int numberOfInfBaselineToFiniteOtherThisWindow = 0;
				int numberOfFiniteBaselineToInfOtherThisWindow = 0;
				int numberOfInfToInfThisWindow = 0;
				int numberOfFiniteToFiniteThisWindow = 0;
				
				for (int q=0; q < windowsQueriesPerformance.get(w).size(); ++q) { // for each query
					PerformanceRecord performanceRecord = windowsQueriesPerformance.get(w).get(q);
					Set<String> allNames = performanceRecord.getAllDesignAlgorithmNames();
					if (allNames.isEmpty()) {
						++emptyQueries;
						continue;						
					}
					String baseline = (ignoreDistributionDistance? findActualSignatureBasedOnDistanceLessSignature(allNames, rawBaseline) : rawBaseline);
					String other = (ignoreDistributionDistance? findActualSignatureBasedOnDistanceLessSignature(allNames, rawOther) : rawOther);
					if (!allNames.contains(baseline) || !allNames.contains(other)) {
						++emptyQueries;
						continue;
					} else {
						++fullQueries;
					}
					if (emptyQueries > 0 && fullQueries > 0) {
						throw new Exception("Window w=" + w +" seems messed up empty=" + emptyQueries + " and full=" + fullQueries);
					}
					
					long baselineMeanLatency = performanceRecord.getPerformanceValueWithDesign(baseline).getMeanActualLatency();
					long baselineStdLatency = performanceRecord.getPerformanceValueWithDesign(baseline).getStdActualLatency();
					
					long otherMeanLatency = performanceRecord.getPerformanceValueWithDesign(other).getMeanActualLatency();
					long otherStdLatency = performanceRecord.getPerformanceValueWithDesign(other).getStdActualLatency();

					//performanceRecord.checkIfPlansAreLegal(cache.getDeployedProjectionNamesToStructures());
					boolean baselineInf = (baselineMeanLatency == Long.MAX_VALUE ? true : false);
					boolean otherInf = (otherMeanLatency == Long.MAX_VALUE ? true : false);
					
					if (baselineInf && otherInf) {
						++numberOfInfToInfThisWindow;							
					} else if (baselineInf && !otherInf) {
						++numberOfInfBaselineToFiniteOtherThisWindow;
					} else if (!baselineInf && otherInf) {
						++numberOfFiniteBaselineToInfOtherThisWindow;
					} else { // neither one is Infinite!
						++numberOfFiniteToFiniteThisWindow;
						baselineTotalWindowLatency += baselineMeanLatency;
						otherTotalWindowLatency += otherMeanLatency;
						double ratio = baselineMeanLatency / (double) otherMeanLatency;
						if (ratio < 0.2 || ratio > 5)
							printDebugInformation(performanceRecord, baseline, other, experimentCache, log.verbosity);
						ratioSum += ratio;
						baselineLatencySum += baselineMeanLatency;
						otherLatencySum += otherMeanLatency;
					}

					log.status(LogLevel.DEBUG, "win="+w+", query="+ q
							+", mean_" + baseline + "="+ baselineMeanLatency +", mean_" + other + "="+ otherMeanLatency
							+", std_" + baseline + "="+ baselineStdLatency + ", std_" + other + "="+otherStdLatency);
				}
				if (emptyQueries>0) {
					continue; // this window is empty!
				}
				
				//int queriesInThisWindow = dvalsWindowsQueriesPerformance.get(d).get(w).size();
				double baselineAvgLatencyThisWindow = baselineTotalWindowLatency/numberOfFiniteToFiniteThisWindow;
				baselineSumAvgLatencyPerWindow += baselineAvgLatencyThisWindow;
				baselineWorstAvgLatencyPerWindow = Math.max(baselineWorstAvgLatencyPerWindow, baselineAvgLatencyThisWindow);
				
				double otherAvgLatencyInThisWindow = otherTotalWindowLatency/numberOfFiniteToFiniteThisWindow;
				otherSumAvgLatencyPerWindow += otherAvgLatencyInThisWindow;
				otherWorstAvgLatencyPerWindow = Math.max(otherWorstAvgLatencyPerWindow, otherAvgLatencyInThisWindow);
				
				maxNumberOfInfBaselineToFiniteOtherPerWindow = Math.max(maxNumberOfInfBaselineToFiniteOtherPerWindow, numberOfInfBaselineToFiniteOtherThisWindow);
				maxNumberOfFiniteBaseToInfOtherlinePerWindow = Math.max(maxNumberOfFiniteBaseToInfOtherlinePerWindow, numberOfFiniteBaselineToInfOtherThisWindow);
				maxNumberOfInfToInfPerWindow = Math.max(maxNumberOfInfToInfPerWindow, numberOfInfToInfThisWindow);
				sumNumberOfInfBaselineToFiniteOtherAllWindows += numberOfInfBaselineToFiniteOtherThisWindow;
				sumNumberOfFiniteBaseToInfOtherlineAllWindows += numberOfFiniteBaselineToInfOtherThisWindow;
				sumNumberOfInfToInfAllWindows += numberOfInfToInfThisWindow;
				sumNumberOfFiniteToFiniteAllWindows += numberOfFiniteToFiniteThisWindow;
			}
			double avgOfLatencyRatios = ratioSum / sumNumberOfFiniteToFiniteAllWindows;
			double ratioOfAverageLatencies = baselineLatencySum / otherLatencySum; 
			log.status(LogLevel.STATUS, "The avg latency of queries with (" + rawBaseline + ") is " + baselineLatencySum / sumNumberOfFiniteToFiniteAllWindows +" but with (" 																							
										+ rawOther + ") is " + otherLatencySum / sumNumberOfFiniteToFiniteAllWindows);
			log.status(LogLevel.STATUS, "Also, the ratio of " + rawBaseline +"/"+ rawOther  + "is: avg of per-query ratios=" + avgOfLatencyRatios + ", ratio of all average latencies=" + ratioOfAverageLatencies);
			double totalWindows = (double)(windowsQueriesPerformance.size()-1);
			log.status(LogLevel.STATUS, "When unit of concern is a window, with (" + rawBaseline + ") we had  (avgWin, maxWin)=(" + baselineSumAvgLatencyPerWindow / totalWindows +" , "+ baselineWorstAvgLatencyPerWindow 
					+ ") but with (" + rawOther + ") (avgWin, maxWin)=(" + otherSumAvgLatencyPerWindow / totalWindows +" , "+ otherWorstAvgLatencyPerWindow + ")");
			log.status(LogLevel.STATUS, "When unit of concern is a window, going from (" + rawBaseline + ") to (" + rawOther + 
					"), we had (avgWin, maxWin)=(" + sumNumberOfInfBaselineToFiniteOtherAllWindows / totalWindows +" , "+ maxNumberOfInfBaselineToFiniteOtherPerWindow 
					+ ") queries go from INF=>finite");
			log.status(LogLevel.STATUS, "When unit of concern is a window, going from (" + rawBaseline + ") to (" + rawOther + 
					"), we had (avgWin, maxWin)=(" + sumNumberOfFiniteBaseToInfOtherlineAllWindows / totalWindows +" , "+ maxNumberOfFiniteBaseToInfOtherlinePerWindow 
					+ ") queries go from finite=>INF");
			log.status(LogLevel.STATUS, "When unit of concern is a window, going from (" + rawBaseline + ") to (" + rawOther + 
					"), we had (avgWin, maxWin)=(" + sumNumberOfInfToInfAllWindows / totalWindows +" , "+ maxNumberOfInfToInfPerWindow 
					+ ") queries go from INF=>INF");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void printDesignTimes(Set<String> allAlgorithmNames, String resultsFilename, ExperimentCache cache) throws Exception {
		PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(resultsFilename);
		List<List<String>> allWindows = plainAlgEval.getOriginalWindowsOfQueryStrings();
		
		log.status(LogLevel.STATUS, "\nDesign Execution Times:************************");
		
		for (String algorithmName : allAlgorithmNames)
			for (int w=1; w < allWindows.size(); ++w) {
				List<DesignExecutionTrace> traces = cache.getDesignExecutionTrace(algorithmName, allWindows.get(w-1));
				if (traces == null || traces.isEmpty()) {
					log.status(LogLevel.STATUS, "For w="+w+" we did not find any execution trace for algorithm " + algorithmName);
					continue;
				}
					
				if (traces.size()!=1)
					log.error("For w=" + w + " and algorithm "+ algorithmName + " you had " + traces.size() + " traces!");
				DesignExecutionTrace trace = traces.get(0);
					
				String msg = "(w=,"+w+","+ algorithmName+ ") took: " + trace.getTotalTimeInSeconds() + " seconds\n";
				msg += ("date: " + trace.getDate());
				//msg += (" msg=" + trace.getMessage()); 
				log.status(LogLevel.STATUS, msg);
			}
		log.status(LogLevel.STATUS, "+++++++++++++++++++\n");
		
		/*
		Map<DesignKey, List<DesignExecutionTrace>> allTraces = cache.getAllDesignExecutionTraces();
		//cache.getDesignExecutionTrace(queries, algorithmName, algorithmParams)
		for (DesignKey key : allTraces.keySet())
			log.status(LogLevel.STATUS, key.toString() + " took " +  allTraces.get(key).get(0).getTotalTimeInSeconds() + " seconds"
					+ " msg= " + allTraces.get(key).get(0).getMessage() + " date= " + allTraces.get(key).get(0).getDate());
		log.status(LogLevel.STATUS, "*************************\n");
		*/
	}

	
	private static void writeDifferentLatenciesToFile(String evaluationFileName, List<String> algorithmNames, String filename) throws IOException {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(filename)));
		try {			
			PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);
			
			List<List<PerformanceRecord>> windowsQueriesPerformance = plainAlgEval.getFinalWindowsQueryPerformance();
			
			bufferedWriter.append("#");
			for (String algorithmName : algorithmNames)
				bufferedWriter.append(algorithmName + "\t");
			bufferedWriter.append("\n");
			
			for (int w=0; w <windowsQueriesPerformance.size(); ++w) {// for each window
				for (int q=0; q < windowsQueriesPerformance.get(w).size(); ++q) { // for each query
					PerformanceRecord performanceRecord = windowsQueriesPerformance.get(w).get(q);
					if (performanceRecord.getAllDesignAlgorithmNames().size()==0) {
						continue; // this is probably from window 0 or another window which doesn't have any 
					}
					for (String algorithmName : algorithmNames) {
						long latency = performanceRecord.getPerformanceValueWithDesign(algorithmName).getMeanActualLatency();
						if (latency == Long.MAX_VALUE)
							latency = -1;
						bufferedWriter.append(latency + "\t");
					}
					bufferedWriter.append("\n");
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		bufferedWriter.close();
	}
	
	private static int printDebugInformation(PerformanceRecord performanceRecord, String baseline, String other, ExperimentCache cache, LogLevel verbosity) throws Exception {
		if (true)
			return 0;
		
		int strange = 0;
			
		BLog mylog = new BLog(verbosity);
		
		StringBuilder msg = new StringBuilder();
		
		PhysicalDesign baselineDesign = performanceRecord.getPerformanceValueWithDesign(baseline).getDesign();
		PhysicalDesign otherDesign = performanceRecord.getPerformanceValueWithDesign(other).getDesign();
		
		Set<PhysicalStructure> baselineSet = new HashSet<PhysicalStructure>(baselineDesign.getPhysicalStructures());
		Set<PhysicalStructure> otherSet = new HashSet<PhysicalStructure>(otherDesign.getPhysicalStructures());
		
		Set<PhysicalStructure> diffSet = new HashSet<PhysicalStructure>(baselineSet);
		diffSet.removeAll(otherSet);
		Set<PhysicalStructure> commonSet = new HashSet<PhysicalStructure>(baselineSet);
		commonSet.removeAll(diffSet);
		
		msg.append("Common projections:\n");
		for (PhysicalStructure p : commonSet) {
			msg.append(p.getHumanReadableSummary() + "\n");
		}
		msg.append("----------------------------------------\n");
		msg.append(baseline + " projections:\n");
		for (PhysicalStructure p : baselineSet) {
			msg.append(p.getHumanReadableSummary() + "\n");
		}
		msg.append("----------------------------------------\n");
		msg.append(other + " projections:\n");
		for (PhysicalStructure p : otherSet) {
			msg.append(p.getHumanReadableSummary() + "\n");
		}
		msg.append("----------------------------------------\n");
		
		for (String algorithm : Arrays.asList(baseline, other)) {
			msg.append("Query: " + performanceRecord.getQuery() + "\n");
			msg.append(algorithm + " Latencies: [");
			for (Long latency : performanceRecord.getPerformanceValueWithDesign(algorithm).getAllActualLatencies())
				msg.append(latency + " ");
			msg.append("] = " + performanceRecord.getPerformanceValueWithDesign(algorithm).getMeanActualLatency());
			msg.append("\n" + algorithm + " Latencies: [");
			for (Long latency : performanceRecord.getPerformanceValueWithDesign(algorithm).getAllActualLatencies())
				msg.append(latency + " ");
			msg.append("] = " + performanceRecord.getPerformanceValueWithDesign(algorithm).getMeanActualLatency());
			////
			List<String> usedProjBaseNames = performanceRecord.getPerformanceValueWithDesign(algorithm).getPhysicalStructureBaseNamesUsedInThePlan(cache.getQueryPlanParser());
			msg.append("\n" + algorithm + " Design: \n");
			List<String> designProjNames = new ArrayList<String>();
			for (PhysicalStructure struct: performanceRecord.getPerformanceValueWithDesign(algorithm).getDesign().getPhysicalStructuresAsList()) {
				String projName = cache.getPhysicalStructureDeployedName(struct);
				msg.append(projName + ", ");
				designProjNames.add(projName);
			}
			if (!designProjNames.containsAll(usedProjBaseNames))
				++strange;
			String planSummary = performanceRecord.getPerformanceValueWithDesign(algorithm).getQueryPlan();
			planSummary = planSummary.substring(0, planSummary.indexOf("PLAN: BASE QUERY PLAN (GraphViz Format)"));
			msg.append("\n" + algorithm + " Query Plan:\n" + planSummary + "\nUsed Projections in " + algorithm + " Plan:\n");
			for (String projBaseName : usedProjBaseNames)
				msg.append(projBaseName + ": " + cache.getDeployedPhysicalStructureBaseNamesToStructures().get(projBaseName).getHumanReadableSummary());
			
		}
				
		mylog.status(LogLevel.DEBUG, msg.toString());
		if (strange != 0)
			throw new RuntimeException("Strange: " + strange);
		return strange;
	}

	private static Map<Integer, Triple<Double, Double, Double>> extractDValues(String topDir, int num_d_values) throws Exception {
		Pattern pattern = Pattern.compile("d(.*)-(.*)_(.*)_(.*)$");		
		File dir = new File(topDir);
		Map<Integer, Triple<Double, Double, Double>> idxToDVal = new HashMap<Integer, Triple<Double,Double,Double>>();
		
		for (File child : dir.listFiles()) {
			if (!child.isDirectory())
				continue;
			String dirName = child.getName();
			Matcher matcher = pattern.matcher(dirName);
			
			if (matcher.find()) {
				System.out.println("Parsing folder name: " + topDir + ", d_idx="+ matcher.group(1)
						+ ", d1=" + matcher.group(2) + ", d2=" + matcher.group(3) + ", d3=" + matcher.group(4));
				
				int d_index = Integer.parseInt(matcher.group(1));
				double d1 = Double.parseDouble(matcher.group(2));
				double d2 = Double.parseDouble(matcher.group(3));			
				double d3 = Double.parseDouble(matcher.group(4));						
				
				if (idxToDVal.containsKey(d_index))
					throw new Exception("d index "+d_index+" appeards more than once.");
				else if (num_d_values>=0 && d_index >= num_d_values)
					continue;
				else
					idxToDVal.put(d_index, new Triple<Double, Double, Double>(d1, d2, d3));
			}
		}
		
		return idxToDVal;
	}

	private static List<DistributionDistance> extractDistancePairValues(
			String topDir, int num_d_values) throws Exception {
		Pattern pattern = Pattern.compile("d(.*)-(.*)_(.*)$");		
		File dir = new File(topDir);
		List<DistributionDistance> dvalues = new ArrayList<DistributionDistance>();
		
		File[] files = dir.listFiles();
		Arrays.sort(files);
		for (File child : files) {
			if (!child.isDirectory())
				continue;
			String dirName = child.getName();
			Matcher matcher = pattern.matcher(dirName);
			
			if (matcher.find()) {
				System.out.println("Parsing folder name: " + topDir + ", d_idx="+ matcher.group(1)
						+ ", d1=" + matcher.group(2) + ", d2=" + matcher.group(3));
				
				int d_index = Integer.parseInt(matcher.group(1));
				double d1 = Double.parseDouble(matcher.group(2));
				double d2 = Double.parseDouble(matcher.group(3));			
				
				if (num_d_values>=0 && d_index >= num_d_values)
					continue;
				else
					dvalues.add(new DistributionDistancePair(d1, d2, 1));
			}
		}
		
		return dvalues;
	}

	private static List<DistributionDistance> extractEuclideanDistanceValues(
			String topDir, int num_d_values) throws Exception {
		Pattern pattern = Pattern.compile("d(\\d+)-(.*)$");		
		File dir = new File(topDir);
		List<DistributionDistance> dvalues = new ArrayList<DistributionDistance>();
		
		File[] files = dir.listFiles();
		Arrays.sort(files);
		for (File child : files) {
			if (!child.isDirectory())
				continue;
			String dirName = child.getName();
			Matcher matcher = pattern.matcher(dirName);
			
			if (matcher.find()) {
				System.out.println("Parsing folder name: " + topDir + ", d_idx="+ matcher.group(1)
						+ ", d=" + matcher.group(2));
				
				try {
					int d_index = Integer.parseInt(matcher.group(1));
					double d = Double.parseDouble(matcher.group(2));
					
					if (num_d_values>=0 && d_index >= num_d_values)
						continue;
					else
						dvalues.add(new EuclideanDistanceWithSimpleUnion(d, 1.0, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption));
				} catch (Exception e) {
					System.out.println("Skipping an inproper directory name: " + dirName);
					continue;
				}
			}
		}
		
		return dvalues;
	}

	
	private static void ensurePlansConformWithDesigns(String evaluationFileName, String cacheFilename) throws Exception {
		PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);
		ExperimentCache cache = ExperimentCache.loadCacheFromFile(cacheFilename);

		Map<String, List<List<PerformanceRecord>>> algorithmWindowsQueryPerformance = plainAlgEval.getAlgorithmsWindowsQueryPerformance();
		
		Map<String, DeployedPhysicalStructure> nameToProjectionMap = cache.getDeployedPhysicalStructureNamesToDeployedPhysicalStructures();
		Map<PhysicalStructure, String> structureToProjectionMap = cache.getPhysicalStructureToDeployedNames();
		
		Set<String> corruptedAlgorithms = new HashSet<String>();
		for (String algorithmName : algorithmWindowsQueryPerformance.keySet()) {
			System.out.println("\n Sanity checking for " + algorithmName);
			boolean failed = false;
			for (int w=1; w<algorithmWindowsQueryPerformance.get(algorithmName).size() && !failed; ++w) {
				System.out.println("\nw=" + w);
				for (int q=0; q<algorithmWindowsQueryPerformance.get(algorithmName).get(w).size() && !failed; ++q) {
					 System.out.print("q=" + q+", ");
					 PhysicalDesign design = algorithmWindowsQueryPerformance.get(algorithmName).get(w).get(q).getPerformanceValueWithDesign(algorithmName).getDesign();
					 List<String> designProjNames = new ArrayList<String>();
					 for (PhysicalStructure projStruct : design.getPhysicalStructures()) {
						 designProjNames.add(cache.getPhysicalStructureToDeployedNames().get(projStruct));
					 }
					 List<String> usedProjNames = algorithmWindowsQueryPerformance.get(algorithmName).get(w).get(q).getPerformanceValueWithDesign(algorithmName).getPhysicalStructureBaseNamesUsedInThePlan(cache.getQueryPlanParser());					 
					 if (!designProjNames.containsAll(usedProjNames))
						 failed = true;
				}
			}
			if (failed)
				corruptedAlgorithms.add(algorithmName);
		}

		if (corruptedAlgorithms.isEmpty())
			System.out.println("No corrupted algorithm");
		else for (String algName : corruptedAlgorithms)
				System.err.println("The following algorithm is corrupted : " + algName);
	}
	

	private static void filterQueriesBasedOnImprovability(String evaluationFileName, ExperimentCache experimentCache, String topDir, String outputFolderPrefix) throws Exception {
		double improvabilityThreshold = 3.0;
		
		PlainAlgorithmEvaluation plainAlgEval = PlainAlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);		
		List<List<PerformanceRecord>> windowsQueriesPerformance = plainAlgEval.getFinalWindowsQueryPerformance();

		String dirName = topDir + File.separator + outputFolderPrefix + File.separator; 
		File directory = new File(dirName);
		if (!directory.exists() && !directory.mkdir())
			throw new Exception("Could not create directory " + dirName);
		
		for (int w=0; w<windowsQueriesPerformance.size(); ++w)  {
			int emptyQueries = 0, fullQueries = 0;
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(dirName + "w"+w+".queries")));
			List<PerformanceRecord> thisWindow = windowsQueriesPerformance.get(w);
			for (PerformanceRecord queryPerformance : thisWindow) {
				String query = queryPerformance.getQuery();
				if (queryPerformance.getAllDesignAlgorithmNames().size()==0) {
					++emptyQueries;
					continue; // we are probably in one of the windows that we are not supposed to be in;
				} else {
					++fullQueries;
				}
				if (emptyQueries > 0 && fullQueries >0)
					throw new Exception("Window w="+w + " seems messed up: emptyQs="+emptyQueries + ", fullQs="+fullQueries);
				long futureLatency = queryPerformance.getPerformanceValueWithDesign(DesignAlgorithm.computeSignatureString(FutureKnowingNominalDesignAlgorithm.class.getSimpleName(), null)).getMeanActualLatency();
				long existingLatency = queryPerformance.getPerformanceValueWithDesign(DesignAlgorithm.computeSignatureString(ExistingNominalDesigner.class.getSimpleName(), "")).getMeanActualLatency();
				long noDesignLatency = queryPerformance.getPerformanceValueWithDesign(DesignAlgorithm.computeSignatureString(NoDesigner.class.getSimpleName(), "")).getMeanActualLatency();
				double improvability = noDesignLatency/(double)futureLatency; 
				if (improvability >= improvabilityThreshold)
					bufferedWriter.append(query+"\n");
			}
			bufferedWriter.close();
		}
		
	}
	

	public static void main(String[] args) throws Exception {
		String db_vendor; // vertica or microsoft
		String database_login_file;
		String deployer_db_alias;
		String designer_db_alias;
		String cliffGuard_config_file;
		String cliffGuard_setting_id = "default";
		String timestampedInputQueryFile;
		String cacheDir;
		String localPathToStatsFile;
		Double distanceValue = 0.0d;
		String output_suggested_design_script_filename;
		Boolean shouldDeploy;
		String output_deployment_script_filename;
		
		String usageMessage = "Usage: " 
				+ "db_vendor "
				+ "db_login_file "
				+ "deployer_db_alias "
				+ "designer_db_alias "
				+ "cliffGuard_config_file "
				+ "cliffGuard_setting_id "
				+ "timestampedInputQueryFile " 
				+ "cache_directory "
				+ "localPathToStatsFile "
				+ "distanceValue(>=0 & <=1) "
				+ "output_suggested_design_script_filename "
				+ "shouldDeploy(t/f)\n "
				+ "[output_deployment_script_filename]";
				
		
		if (args.length < 12 || args.length > 13) {
			log.error(usageMessage);
			return;
		}
		try {
			String homeDir = System.getProperty("user.home");
			int idx = 0;
			// we have the right number of parameters
			db_vendor = args[idx++]; db_vendor = db_vendor.toLowerCase();
			assert db_vendor.equals("vertica") || db_vendor.equals("microsoft");
			
			database_login_file = args[idx++];
			if (database_login_file.startsWith("~/"))
				database_login_file = homeDir + File.separator + database_login_file.substring(2);
			
			deployer_db_alias = args[idx++];
			
			designer_db_alias = args[idx++];
			assert !deployer_db_alias.equals(designer_db_alias);
			
			cliffGuard_config_file = args[idx++];
			cliffGuard_setting_id = args[idx++];
			timestampedInputQueryFile = args[idx++];
			
			cacheDir = args[idx++];
			if (cacheDir.startsWith("~/"))
				cacheDir = homeDir + File.separator + cacheDir.substring(2);
	
			localPathToStatsFile = args[idx++]; 
			
			distanceValue = Double.parseDouble(args[idx++]);
			
			output_suggested_design_script_filename = args[idx++];
	
			shouldDeploy = args[idx++].equals("t") ? true : false;
			
			output_deployment_script_filename = args.length >= 13 ? args[idx++] : "";
			
			if (!shouldDeploy && output_deployment_script_filename.isEmpty()) {
				log.error("shouldDeploy is set to false. You need to specify output_deployment_script_filename");
				return;
			}
				
			log.status(LogLevel.STATUS, "Running with the following parameters:\n"
					+ "\ndb_vendor=" + db_vendor
					+ "\ndatabase_login_file=" + database_login_file
					+ "\ndeployer_db_alias=" + deployer_db_alias
					+ "\ndesigner_db_alias=" + designer_db_alias
					+ "\ncliffGuard_config_file=" + cliffGuard_config_file
					+ "\ncliffGuard_setting_id=" + cliffGuard_setting_id
					+ "\ntimestampedInputQueryFile=" + timestampedInputQueryFile
					+ "\ncache_directory=" + cacheDir
					+ "\nlocalPathToStatsFile=" + localPathToStatsFile
					+ "\ndistanceValue=" + distanceValue
					+ "\noutput_suggested_design_script_filename=" + output_suggested_design_script_filename
					+ "\nshouldDeploy=" + shouldDeploy
					+ "\noutput_deployment_script_filename=" + output_deployment_script_filename
					+ "\n\n"
					);
		
			String DBVendor;
			List<DatabaseLoginConfiguration> allDatabaseConfigurations;
			if (db_vendor.equalsIgnoreCase("microsoft")) {
				allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(database_login_file, MicrosoftDatabaseLoginConfiguration.class.getSimpleName());
				DBVendor = MicrosoftDatabaseLoginConfiguration.class.getSimpleName();
			} else if (db_vendor.equalsIgnoreCase("vertica")) {
				allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(database_login_file, VerticaDatabaseLoginConfiguration.class.getSimpleName());
				DBVendor = VerticaDatabaseLoginConfiguration.class.getSimpleName();
			} else
				throw new Exception("Unsupported vendor: " + db_vendor);
	
			DatabaseLoginConfiguration deployerConfig = DatabaseLoginConfiguration.findDBAliasInList(deployer_db_alias, allDatabaseConfigurations);
			DatabaseLoginConfiguration designerConfig = DatabaseLoginConfiguration.findDBAliasInList(designer_db_alias, allDatabaseConfigurations);
			if (deployerConfig==null || designerConfig==null || !deployerConfig.getDBname().equals(designerConfig.getDBname()))
				throw new Exception("Your aliases either do not exist or refer to databases of different names!");
			
			QueryParser queryParser = new Query_SWGO.QParser();
			//TODO: In this version, we do not check whetehr the two DB aliases have the same schema or not... We need to add this feature in the future.
			Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMap(deployer_db_alias, allDatabaseConfigurations).getSchemas();
			
			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", queryParser, schemaMap);
			List<String> allQueryStrings = sqlLogFileManager.loadTimestampQueryStringsFromFile(timestampedInputQueryFile);
			System.out.println("# of Queries that will be used in our design = " + allQueryStrings.size() + " out of which # of uniques are "
					+ new HashSet(allQueryStrings).size());
			
			
			//NonConvexDesigner nonConvexDesigner = NonConvexDesigner.loadNonConvexDesignerFromFile(cliffGuard_config_file);

			String cacheFilename = cacheDir + File.separator + "experiment.cache";
			String newCacheFileName = null;
			
			newCacheFileName = performDesign(designerConfig, deployerConfig, cliffGuard_config_file, cliffGuard_setting_id, distanceValue, allQueryStrings, localPathToStatsFile, cacheFilename, output_suggested_design_script_filename, shouldDeploy, output_deployment_script_filename); 
	
			
			Timer t = new Timer();
			ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile(newCacheFileName);
			System.out.println("Loading experiement cache took " + t.lapSeconds() + " secs");
			t = new Timer();
			experimentCache.saveTheEntireCache(cacheDir + File.separator + "experiment.cache");
			System.out.println("Copying experiement cache took " + t.lapSeconds() + " secs");
			
			log.status(LogLevel.STATUS, "CliffGuard is now done.");
		} catch (Exception e) {
			log.error(e.toString());
			e.printStackTrace();
		}
	}

}
