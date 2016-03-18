package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Random;
import java.util.List;

//import edu.mit.robustopt.clustering.QueryGenerator;
import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.Randomness;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.Generator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

import com.relationalcloud.tsqlparser.loader.Schema;

public class EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong 
	extends LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses {
	public final static Set<UnionOption> AllClausesOption = new HashSet<UnionOption> (){{  
        add(UnionOption.SELECT);  
        add(UnionOption.WHERE);  
        add(UnionOption.GROUP_BY);
        add(UnionOption.ORDER_BY);
	}};

	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(Map<String, Schema> schemaMap, 
			List<String> exampleSqlQueries, Set<UnionOption> option) throws Exception {
		super(schemaMap, exampleSqlQueries, option);
	}
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(
			String dbAlias, String DBVendor, List<String> exampleSqlQueries, Set<UnionOption> option) throws Exception {
		super(dbAlias, DBVendor, exampleSqlQueries, option);
	}
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(String dbAlias,
			List<DatabaseLoginConfiguration> allDatabaseConfigurations,
			List<String> exampleSqlQueries, Set<UnionOption> option) throws Exception {
			super(dbAlias, allDatabaseConfigurations, exampleSqlQueries, option);
			if (option.isEmpty()) {
				throw new Exception("Should at least has a clause in option");
			}
	}

	@Override
	public ClusteredWindow forecastNextWindow(ClusteredWindow curWindow,
			EuclideanDistance distance) throws Exception {
		if (distance == null) {
			throw new Exception("You need to provide a EuclideanDistance");
		}
		penaltyForGoingFromZeroToNonZero = distance.getPenaltyForGoingFromZeroToNonZero();
		if (curWindow == null) {
			throw new Exception("We cannot forecast the next window from a null window!");
		}
		List<Query> bestListSoFar = null;
		List<Query> currentQueries = curWindow.getAllQueries();
		List<Query> currentList = curWindow.getAllQueries();
		Double bestDistanceSoFar = 0d;
		double dist = distance.getDistance();
		int curWindowSize = curWindow.totalNumberOfQueries();
		EuclideanDistanceWithSimpleUnion ed;
		EuclideanDistanceWithSimpleUnion.Generator distanceGenerator = (EuclideanDistanceWithSimpleUnion.Generator) getDistributionDistanceGenerator();
		Clustering clusteringQueryEquality = getClustering();
		
		Pair<List<Query>, Double> pair = findAWindowWithLargeDist(currentQueries, dist, distanceGenerator);
		bestDistanceSoFar = pair.second;
		bestListSoFar = pair.first;
		if (bestDistanceSoFar <= dist) {
			return clusteringQueryEquality.cluster(bestListSoFar);
		}
		int numOfQueries = estimatedHowManyQueriesToSubstitute(dist, bestDistanceSoFar, curWindowSize);
		substituteQueries(currentQueries, bestListSoFar, numOfQueries);
		ed = distanceGenerator.distance(bestListSoFar, currentList);
		bestDistanceSoFar = ed.getDistance();
		if (bestDistanceSoFar <= dist) {
			return clusteringQueryEquality.cluster(bestListSoFar);
		}
		while (bestDistanceSoFar > dist) {
			numOfQueries = estimateHowManyQueriesToAdd(dist, bestDistanceSoFar, curWindowSize);
			if (!addQueries(currentQueries, bestListSoFar, numOfQueries)) {
				bestListSoFar.remove(0);
			}
			ed = distanceGenerator.distance(bestListSoFar, currentList);
			bestDistanceSoFar = ed.getDistance();
		}
		return clusteringQueryEquality.cluster(bestListSoFar);
	}
	
	protected Pair<List<Query>, Double> findAWindowWithLargeDist(List<Query> curWindow, 
			Double dist, EuclideanDistanceWithSimpleUnion.Generator distanceGenerator) throws Exception {
		 /* find a window with same total frequency with curWindow from log file
		  * whose distance with curWindow is larger than dist
		  * return the new distance and new window
		  */
		Double newDist = Double.NaN;
		int windowSize = curWindow.size();
		Clustering clusteringQueryEquality = getClustering();
		int listSize = exampleSqlQueries.size();
		int numOfTrials = 50;
		int upperThreshold = 4;
		double lowerThreshold = 0.95;
		
		for (int count = 0; count < numOfTrials; count++) {
			if (listSize <= windowSize) {
				exampleSqlQueries.addAll(exampleSqlQueries);
				listSize *= 2;
			}
			int r = Randomness.randGen.nextInt(listSize - windowSize);
			int i = windowSize + r;
			List<Query> queries = new ArrayList<Query>(exampleSqlQueries.subList(i - windowSize, i));
			EuclideanDistance newEDist = distanceGenerator.distance(curWindow, queries);
			newDist = newEDist.getDistance();
			if((newDist > dist) || (newDist < dist && newDist > lowerThreshold * dist)){//can add 0.95 as parameter
				return new Pair<List<Query>, Double> (queries, newDist);
			} 
		}
		throw new Exception("Hard to find such window in the log file, make sure the distance is correct");
	}
	
	protected int estimateHowManyQueriesToAdd(double targetDistance, double distanceSoFar, int queriesInCurWindow) {
		int value = (int) (queriesInCurWindow * (distanceSoFar - targetDistance) / (distanceSoFar + targetDistance)) + 1;
		return value;
	}
	
	protected int estimatedHowManyQueriesToSubstitute(double targetDistance, double distanceSoFar, int queriesInCurWindow) {
		return (int) (queriesInCurWindow * (1 - (targetDistance / distanceSoFar)) / 2);
	}
	
	protected boolean addQueries(List<Query> curWindow, List<Query> nextWindow, int numOfQueries) throws Exception {
		int curWindowSize = curWindow.size();
		int sentinel = curWindow.size() - numOfQueries;
		if (sentinel >= 0) {
			nextWindow.addAll(curWindow.subList(curWindowSize - numOfQueries, curWindowSize));
		} else {
			nextWindow.addAll(curWindow);
		}
		//remove the last numOfQueries queries from curWindowQueries
		for (int i = curWindow.size() - 1; i >= sentinel; i-- ) {
			if (i < 0) {
				return false;
			}
			curWindow.remove(i);
		}
		return true;
	}
	
	protected void substituteQueries(List<Query> curWindow, List<Query> nextWindow, int numOfQueries) throws Exception {
		//substitute queries in window with queries in current window
		int curWindowSize = curWindow.size();
		//remove the last numOfQueries queries from nextWindowQueries
		int sentinel = nextWindow.size() - numOfQueries;
		for (int i = nextWindow.size() - 1; i >= sentinel; i-- ) {
			nextWindow.remove(i);
		}
		nextWindow.addAll(curWindow.subList(curWindowSize - numOfQueries, curWindowSize));
		//remove the last numOfQueries queries from curWindowQueries
		for (int i = curWindow.size() - 1; i >= sentinel; i-- ) {
			curWindow.remove(i);
		}
	}
	
	public static void unitTest1() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_wide.txt";
		String s1 = "SELECT col1 FROM wide100 WHERE col2>1;";
		String s2 = "SELECT col2 FROM wide100 WHERE col3>1;";
		String s3 = "SELECT col3 FROM wide100 WHERE col4>1;";
		String s4 = "SELECT col4 FROM wide100 WHERE col5>3;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2); w1.add(s3); w1.add(s3); w1.add(s4);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		double penalty = 1.5d;
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.04, penalty, 1, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		//got everything set up
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistance distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
		
		ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
		List<Query> qlistWindow3 = window3.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify2 = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow3);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify2);
		System.out.println(window3);
	}
	
	public static void unitTest2() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbAlias = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}}  ;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option);
		String s1 = "SELECT * FROM fnma.ident_71 WHERE ident_651 > 1;";
		String s2 = "SELECT * FROM rcondon.ident_127 WHERE ident_1385 > 1;";
		String s3 = "SELECT * FROM public.shubh_test WHERE ident_2071 > 1;";
		String s4 = "SELECT * FROM qa.ident_66 WHERE ident_1398 > 1;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2); w1.add(s3); w1.add(s4);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.0002, 1d, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		System.out.println("distance1 = " + dist1);
		System.out.println("Start to forcast next query...");
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1, dist1);
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, 1d, option).distance(qlist1, qlistWindow2);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}
	
	public static void unitTest3() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_wide.txt";
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_wide.txt";
		int maxQueriesPerWindow = 100;
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		double penalty = 2d;
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.18, penalty, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}
	
	public static void unitTest35() throws Exception {
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		
		String dbAlias = "dataset19";
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String unionSqlQueriesFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/" + "parsed-runnable-improvable.timestamped";
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w2.queries";
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<Query_SWGO> allPossibleQueries = sqlLogFileManager.loadTimestampQueriesFromFile(unionSqlQueriesFile);
		List<String> allPossibleSqlQueries = new ArrayList<String>();
		for (Query_SWGO q : allPossibleQueries)
			allPossibleSqlQueries.add(q.getSql());
				
		List<String> wq1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, 10000);
		List<Query_SWGO> swgo_qlist1 = new Query_SWGO.QParser().convertSqlListToQuery(wq1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(swgo_qlist1);
		double avgDistance = 0.006;
		
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(avgDistance);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(schemaMap, allPossibleSqlQueries, option);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(swgo_qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query..."); 
		Timer t = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}

	public static void unitTest4() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbAlias = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		System.out.println("Initializing workload generator...");
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}}  ;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option);
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_dataset19.txt";
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.00044);
		System.out.println(window1);
		System.out.println("Start to forcast next workload...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}
	
	public static void unitTest5() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbAlias = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option);
		String s1 = "SELECT * FROM testusr.ident_71 WHERE ident_651 = 1;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Cluster c = new Cluster(qlist1);
		Cluster newCluster = workloadgenerator.createClusterWithNewFrequency(c, 3);
		System.out.println(newCluster.getAllSql());
	}
	

	public static void  dataset19_test() throws Exception {
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		
		String dbAlias = "dataset19";
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String unionSqlQueriesFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/" + "parsed-runnable-improvable.timestamped";
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<Query_SWGO> allPossibleQueries = sqlLogFileManager.loadTimestampQueriesFromFile(unionSqlQueriesFile);
		List<String> allPossibleSqlQueries = new ArrayList<String>();
		for (Query_SWGO q : allPossibleQueries)
			allPossibleSqlQueries.add(q.getSql());
				
		
		List<String> windows = new ArrayList<String>();
		String windowFile0 = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w0.queries";

		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong gen = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleSqlQueries, option);
	
	}
	
	public static void main(String args[]) {
		try {			
			unitTest35();
//			unitTest1();
//			unitTest2();
//			unitTest3();
//			unitTest4();
//			unitTest5();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public DistributionDistanceGenerator<EuclideanDistance> getDistributionDistanceGenerator()
			throws Exception {
		return new EuclideanDistanceWithSimpleUnion.Generator(schema, penaltyForGoingFromZeroToNonZero, whichClauses);
	}
}


