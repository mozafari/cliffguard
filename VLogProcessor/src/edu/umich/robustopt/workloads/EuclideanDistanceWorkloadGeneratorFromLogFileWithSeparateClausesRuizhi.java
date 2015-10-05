package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.List;


//import edu.mit.robustopt.clustering.QueryGenerator;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.Randomness;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSeparateClauses.Generator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

import com.relationalcloud.tsqlparser.loader.Schema;

public class EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi
	extends LogFileBasedEuclideanDistanceWorkloadGenerator {

	private int numOfNewQueries;
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(Map<String, Schema> schemaMap, 
			List<String> exampleSqlQueries, int numOfNewQueries) throws Exception {
		super(schemaMap, exampleSqlQueries); // there's no option to pass to the top level code!
		this.numOfNewQueries = numOfNewQueries;
	}
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(
			String dbName, String DBVendor, List<String> exampleSqlQueries, int numOfNewQueries) throws Exception {
		super(dbName, DBVendor, exampleSqlQueries);
		this.numOfNewQueries = numOfNewQueries;
	}

	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(
		String dbName, List<DatabaseLoginConfiguration> dbLogins, List<String> exampleSqlQueries, int numOfNewQueries) throws Exception {
		super(dbName, dbLogins, exampleSqlQueries);
		this.numOfNewQueries = numOfNewQueries;
	}

	@Override
	public DistributionDistanceGenerator<EuclideanDistance> getDistributionDistanceGenerator()
			throws Exception {
		return new EuclideanDistanceWithSeparateClauses.Generator(schema, penaltyForGoingFromZeroToNonZero);
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
		int listSize = exampleSqlQueries.size();
		if (listSize < numOfNewQueries) {
			throw new Exception("Number of new queries in next workload can't be larger than "
					+ "the number of queries in log file" + listSize + " < " + numOfNewQueries);
		}
		double dist = distance.getDistance();
		int curWindowSize = curWindow.totalNumberOfQueries();
		EuclideanDistanceWithSeparateClauses.Generator distanceGenerator = (EuclideanDistanceWithSeparateClauses.Generator) getDistributionDistanceGenerator();
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		List<Query> listOfCurWindow = curWindow.getAllQueries();
		Set<Query> setOfCurWindow = new HashSet<Query>();
		
		for(Query q : listOfCurWindow){
			setOfCurWindow.add(q);
		}
		
		// get a far away window
		int numOfTrials = 10000;
		double muchLargerDistanceThanRequested = 0;
		List<Query> qList = new ArrayList<Query>();
		
		int count = 0;
		double maxSeenSofar = -1.0;
		for (; count < numOfTrials; count++) {
			qList = new ArrayList<Query>();
			for (int i = 0; i < numOfNewQueries; i++) {
				int randomC = Randomness.randGen.nextInt(listSize);
				Query q = exampleSqlQueries.get(randomC);
				if(!setOfCurWindow.contains(q))
					qList.add(q);
				else
					i--;
			}
			EuclideanDistanceWithSeparateClauses ed = distanceGenerator.distance(qList, listOfCurWindow);
			muchLargerDistanceThanRequested = ed.getDistance();
			if (muchLargerDistanceThanRequested < dist) {
				maxSeenSofar = (maxSeenSofar < muchLargerDistanceThanRequested ? muchLargerDistanceThanRequested : maxSeenSofar);
				continue;
			} else {
				break;
			}
			
		}
			
		if (count >= numOfTrials) {
			throw new Exception("The log file is too similar with the current window or WorkloadGeneratorRuizhi not applicable: " + maxSeenSofar + " < " + dist + "\nAnd CurrentWindow is " + curWindow);
		}
		 
		//found a window with large distance
		double lambda = Math.sqrt(dist / muchLargerDistanceThanRequested);
		int copyN =(int) (curWindowSize * lambda / ((1.0 - lambda) * numOfNewQueries));
		for (int k = 0; k < numOfNewQueries; k++) {
			Query q = qList.get(k);
			for(int i = 0; i < copyN; ++ i){
				listOfCurWindow.add(q);
			}
		}
		return clusteringQueryEquality.cluster(listOfCurWindow);
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
		List<String> exampleSqlQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		double penalty = 1.5d;
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses(0.14, penalty, 1);
		//got everything set up
		int numOfNewQueries = 2;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleSqlQueries, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistance distverify = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap, penalty).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is "+distverify);
		System.out.println(window2);
		
		ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
		List<Query> qlistWindow3 = window3.getAllQueries();
		EuclideanDistanceWithSeparateClauses distverify2 = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap, penalty).distance(qlist1, qlistWindow3);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify2);
		System.out.println(window3);
	}
	
	public static void unitTest2() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/dataset_synthetic_tpch/"; 
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String dbName = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/parsed.plain";
		List<Boolean> SWGO = Arrays.asList(true, true, true, true); 
		int maxQueriesPerWindow = 1000;
		List<String> exampleSqlQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleSqlQueries, numOfNewQueries);
		String s1 = "SELECT min(ident_651), ident_2645 FROM fnma.ident_71 WHERE ident_651 > 1 GROUP BY ident_1404, ident_2645 ORDER BY ident_1773, ident_1526;";
		String s2 = "SELECT * FROM rcondon.ident_127 WHERE ident_1385 > 1;";
		String s3 = "SELECT * FROM public.shubh_test WHERE ident_2071 > 1;";
		String s4 = "SELECT * FROM qa.ident_66 WHERE ident_1398 > 1;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2); w1.add(s3); w1.add(s4);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses(0.0002);
		System.out.println("distance1 = " + dist1);
		System.out.println("Start to forcast next query...");
		
		int count = 0;
		List<ClusteredWindow> perturbedWindows = new ArrayList<ClusteredWindow>();
		while (true) {
			ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
			List<Query> qlistWindow3 = window3.getAllQueries();
			EuclideanDistanceWithSeparateClauses distverify = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap).distance(qlist1, qlistWindow3);
			System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
			System.out.println(window3);
			perturbedWindows.add(window3);
			Set<ClusteredWindow> deduplicatedWindows = new HashSet<ClusteredWindow>(perturbedWindows);
			if (deduplicatedWindows.size() < perturbedWindows.size())
				break;
			else
				count++;
		}
		System.out.println("count = " + count);
		
	}
	
	public static void unitTest3() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_wide.txt";
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_wide.txt";
		int maxQueriesPerWindow = 100;
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		List<String> exampleSqlQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses(0.18, 2d);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleSqlQueries, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		
		int count = 0;
		List<ClusteredWindow> perturbedWindows = new ArrayList<ClusteredWindow>();
		while (true) {
			ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
			List<Query> qlistWindow3 = window3.getAllQueries();
			EuclideanDistanceWithSeparateClauses distverify = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap).distance(qlist1, qlistWindow3);
			System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
			System.out.println(window3);
			perturbedWindows.add(window3);
			Set<ClusteredWindow> deduplicatedWindows = new HashSet<ClusteredWindow>(perturbedWindows);
			if (deduplicatedWindows.size() < perturbedWindows.size())
				break;
			else
				count++;
		}
		System.out.println("count = " + count);
	}
	
	public static void unitTest35() throws Exception {
		
		String dbName = "dataset19";
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String unionSqlQueriesFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/" + "parsed-runnable-improvable.timestamped";
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w2.queries";
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<Query_SWGO> exampleQueries = sqlLogFileManager.loadTimestampQueriesFromFile(unionSqlQueriesFile);
		List<String> exampleSqlQueries = new ArrayList<String>();
		for (Query_SWGO q : exampleQueries)
			exampleSqlQueries.add(q.getSql());
				
		List<String> wq1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, 10000);
		List<Query_SWGO> swgo_qlist1 = new Query_SWGO.QParser().convertSqlListToQuery(wq1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(swgo_qlist1);
		double avgDistance = 4.945309816428576E-4;
		
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses(avgDistance, 0d, 1);
		int numOfNewQueries = 3;
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(schemaMap, exampleSqlQueries, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(swgo_qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query..."); 
		Timer t = new Timer();
		ClusteredWindow window2 = null;
		for (int i=0; i<1000; ++i) {
			window2 = workloadgenerator.forecastNextWindow(window1,dist1);
			if (window2 == null)
				System.err.println("ERROR: " + i);
		}
		
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSeparateClauses distverify = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap, 0d).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}

	public static void unitTest4() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbName = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		int maxQueriesPerWindow = 1000;
		List<String> exampleSqlQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		System.out.println("Initializing workload generator...");
		int numOfNewQueries = 3;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSeparateClausesRuizhi(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleSqlQueries, numOfNewQueries);
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_dataset19.txt";
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses(0.0003, 0d);
		System.out.println(window1);
		System.out.println("Start to forcast next workload...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSeparateClauses distverify = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap, 0d).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}
	
	public static void main(String args[]) {
		try {
//			unitTest35();
//			unitTest1();
			unitTest2();
//			unitTest3();
//			unitTest4();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}


