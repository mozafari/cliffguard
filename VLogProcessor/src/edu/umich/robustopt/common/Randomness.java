package edu.umich.robustopt.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;



import edu.umich.robustopt.algorithms.NonConvexDesigner;
import edu.umich.robustopt.algorithms.RobustDesigner;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.vertica.VerticaConnection;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaDeployer;
import edu.umich.robustopt.vertica.VerticaDesignMode;
import edu.umich.robustopt.vertica.VerticaDesignParameters;
import edu.umich.robustopt.vertica.VerticaDesigner;
import edu.umich.robustopt.vertica.VerticaLatencyMeter;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistancePair;
import edu.umich.robustopt.workloads.WideTableWorkloadGenerator;
import edu.umich.robustopt.workloads.WorkloadGenerator;

/*
 * This class is the source of randomness in my entire program! Anyone who wants to use random numbers 
 * should call the functions of this class!
 */
public class Randomness {
	public static long randSeed = (GlobalConfigurations.RANDOM_SEED == null ? System.currentTimeMillis() : GlobalConfigurations.RANDOM_SEED);
	public static Random randGen = new Random(randSeed);


	public static void main(String[] args) throws Exception {
		boolean deterministic = isDeterministic();
		System.out.println("Your code " + (deterministic ? "is" : "is NOT") + " determinisitc.");
	}

	public static boolean isDeterministic() throws Exception {
		if (!testGeneratePurturbedWindows())
			throw new Exception("Non-deterministic");
		
		
		System.out.println("The current code seems deterministic!");
		return true;
	}
	
	public static boolean testGeneratePurturbedWindows() throws Exception {
		Map<DistributionDistance, List<List<String>>> dvalsWindowsQueries = getDvalsWindowsQueries();
		int howManyPurturbation = 10;
		boolean deterministic = true;
		
		final VerticaDesignParameters designParameters = new VerticaDesignParameters(VerticaDesignMode.BALANCED);
		String cacheFilename = "/tmp/expCache";
		ExperimentCache experimentCache = new ExperimentCache(cacheFilename, 1, 1, 1, new VerticaQueryPlanParser());
		
		String dbName = "wide";
		
		String vise4_vm_tpch_stats_file = "/home/dbadmin/robust-opt/dataset_synthetic_tpch/tpch_stats_scale10.xml";	
		String wide_stats_file = "/home/dbadmin/robust-opt/dataset_wide/wide100_stats.xml";

		DBDesigner dbDesigner = new VerticaDesigner(LogLevel.STATUS, VerticaConnection.createDefaultDBLoginByNameAndServerAlias(dbName, "vm_empty_db"), wide_stats_file, experimentCache);
		DBDeployer dbDeployer = new VerticaDeployer(LogLevel.STATUS, VerticaConnection.createDefaultDBLoginByNameAndServerAlias(dbName, "real_full_db"), experimentCache, false);
		
		DatabaseInstance dbDeployment = dbDeployer;
		
		LatencyMeter latencyMeter = new VerticaLatencyMeter(LogLevel.VERBOSE, true, VerticaConnection.createDefaultDBLoginByNameAndServerAlias(dbName, "real_full_db"), experimentCache, dbDeployment, null, 10*60);
				
		Map<DistributionDistance, List<List<PerformanceRecord>>> dvalsWindowsQueryPerformance = new HashMap<DistributionDistance, List<List<PerformanceRecord>>>();
		for (DistributionDistance d : dvalsWindowsQueries.keySet()) {
			dvalsWindowsQueryPerformance.put(d, new ArrayList<List<PerformanceRecord>>());
			for (int w=0; w<dvalsWindowsQueries.get(d).size(); ++w) {
				dvalsWindowsQueryPerformance.get(d).add(new ArrayList<PerformanceRecord>());
				for (int q=0; q<dvalsWindowsQueries.get(d).get(w).size(); ++q) {
					PerformanceRecord peformanceRecord = new PerformanceRecord(dvalsWindowsQueries.get(d).get(w).get(q));
					dvalsWindowsQueryPerformance.get(d).get(w).add(peformanceRecord);
				}
			}
		}

		//WorkloadGenerator workloadGen = new SimpleTPCHSyntheticWorkloadGenerator();
		WorkloadGenerator workloadGen = new WideTableWorkloadGenerator(VerticaDatabaseLoginConfiguration.class.getSimpleName());
		
		int seed = 1234567890;
		DistributionDistance dist = null;
		for (DistributionDistance d : dvalsWindowsQueries.keySet()) {
			dist = d;
			break;
		}
		List<String> sqlQueries = dvalsWindowsQueries.get(dist).get(0);
			
		List<ClusteredWindow> prevResult = null;
		for (int i=0; i<4; ++i) {
			Random myRandGen = new Random(seed);
			Randomness.randGen = myRandGen;
			
			ClusteredWindow clusteredWindow = workloadGen.getClustering().convertSqlListIntoClusteredWindow(sqlQueries, workloadGen);
			
			String before = clusteredWindow.toString();
			List<ClusteredWindow> newResult = RobustDesigner.generatePurturbedWindows(workloadGen, howManyPurturbation, new BLog(LogLevel.STATUS), clusteredWindow, dist);
			String after = clusteredWindow.toString();
			if (!after.equals(before))
				throw new Exception("Before: \n"+before+"\nBut after=\n"+after);
			
			if (i==0)
				prevResult = newResult;
			else if (!newResult.equals(prevResult)) {
				System.out.println("prevResult:\n"+prevResult + "\nBut the new result is:\n" + newResult);
				System.err.println("RobustDesigner.generatePurturbedWindows is NOT deterministic!");
				return false;
			}
		}
		System.out.println("RobustDesigner.generatePurturbedWindows is deterministic");
		
		int numberOfQuerySamples = 0; // to make sure that they use all queries!
		
		numberOfQuerySamples = 20;
		
		double initialWeight = 1.0, successFactor = 0.5, failureFactor = 1.2;
		int numberOfIterations = 20, maximumQueriesPerWindow = 200;
		double maxFractionOfWorstSolutions = 0.3; 
		double avgDistanceFactorToFormAGap = 3.0; 
		double noticeableRelativeDifference = 0.01;
		
		NonConvexDesigner nonConvexDesigner = new NonConvexDesigner(4, LogLevel.DEBUG, dbDesigner, dbDeployer, designParameters, experimentCache, workloadGen, howManyPurturbation, numberOfIterations, 
				initialWeight, successFactor, failureFactor, true, 
				maxFractionOfWorstSolutions, avgDistanceFactorToFormAGap, noticeableRelativeDifference, latencyMeter, false, maximumQueriesPerWindow, new DistributionDistancePair.Generator());

	
		return true;
	}
	
	private static Map<DistributionDistance, List<List<String>>> getDvalsWindowsQueries () {
		Map<DistributionDistance, List<List<String>>> dvalsWindowsQueries = new HashMap<DistributionDistance, List<List<String>>>();
		DistributionDistance dist = new DistributionDistancePair(0.1, 0.3, 1);
		
		List<String> win1 = new ArrayList<String>(), win2 = new ArrayList<String>();
		List<List<String>> windows = new ArrayList<List<String>>();
		
		win1.add("SELECT min(col30) FROM public.wide100 WHERE col98 <= 1146 AND col30 <= 7995 LIMIT 10;");
		win1.add("SELECT min(col42) FROM public.wide100 WHERE col42 <= 1154 AND col29 <= 1170 LIMIT 10;");
		win1.add("SELECT min(col42) FROM public.wide100 WHERE col42 <= 1154 AND col29 <= 1170 LIMIT 10;");
		windows.add(win1);
		
		win2.add("SELECT min(col42) FROM public.wide100 WHERE col42 <= 1154 AND col29 <= 1170 LIMIT 10;");
		win2.add("SELECT min(col18) FROM public.wide100 WHERE col18 <= 7998 AND col43 <= 1146 LIMIT 10;");
		win2.add("SELECT min(col18) FROM public.wide100 WHERE col18 <= 7998 AND col43 <= 1146 LIMIT 10;");
		windows.add(win2);

		dvalsWindowsQueries.put(dist, windows);
		
		return dvalsWindowsQueries;
	}
}
