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
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.Generator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

import com.relationalcloud.tsqlparser.loader.Schema;

public class EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui
	extends LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses {
	public final static Set<UnionOption> AllClausesOption = new HashSet<UnionOption> (){{  
        add(UnionOption.SELECT);  
        add(UnionOption.WHERE);  
        add(UnionOption.GROUP_BY);
        add(UnionOption.ORDER_BY);
	}};

	private int numOfNewQueries;
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(Map<String, Schema> schemaMap, 
			List<String> exampleSqlQueries, Set<UnionOption> whichClauses, int numOfNewQueries) throws Exception {
		super(schemaMap, exampleSqlQueries, whichClauses);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.numOfNewQueries = numOfNewQueries;
	}
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(
			String dbName, String DBVendor, List<String> exampleSqlQueries, Set<UnionOption> whichClauses, int numOfNewQueries) throws Exception {
		super(dbName, DBVendor, exampleSqlQueries, whichClauses);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.numOfNewQueries = numOfNewQueries;
	}

	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(
		String dbName, List<DatabaseLoginConfiguration> dbLogins, List<String> exampleSqlQueries, Set<UnionOption> whichClauses, int numOfNewQueries) throws Exception {
		super(dbName, dbLogins, exampleSqlQueries, whichClauses);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.numOfNewQueries = numOfNewQueries;
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
		EuclideanDistanceWithSimpleUnion.Generator distanceGenerator = (EuclideanDistanceWithSimpleUnion.Generator) getDistributionDistanceGenerator();
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
			EuclideanDistanceWithSimpleUnion ed = distanceGenerator.distance(qList, listOfCurWindow);
			muchLargerDistanceThanRequested = ed.getDistance();
			if (muchLargerDistanceThanRequested < dist) {
				maxSeenSofar = (maxSeenSofar < muchLargerDistanceThanRequested ? muchLargerDistanceThanRequested : maxSeenSofar);
				continue;
			} else {
				break;
			}
			
		}
			
		if (count >= numOfTrials) {
			throw new Exception("The log file is too similar with the current window or WorkloadGeneratorJingkui not applicable: " + maxSeenSofar + " < " + dist + "\nAnd CurrentWindow is " + curWindow);
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
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		double penalty = 1.5d;
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.14, penalty, 1, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		//got everything set up
		int numOfNewQueries = 2;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistance distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is "+distverify);
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
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/dataset_synthetic_tpch/"; 
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String dbName = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		List<Boolean> SWGO = Arrays.asList(true, true, true, true); 
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
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
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.0002);
		System.out.println("distance1 = " + dist1);
		System.out.println("Start to forcast next query...");
		
		int count = 0;
		List<ClusteredWindow> perturbedWindows = new ArrayList<ClusteredWindow>();
		while (true) {
			ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
			List<Query> qlistWindow3 = window3.getAllQueries();
			EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow3);
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
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}}  ;
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.18, 2d, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		
		int count = 0;
		List<ClusteredWindow> perturbedWindows = new ArrayList<ClusteredWindow>();
		while (true) {
			ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
			List<Query> qlistWindow3 = window3.getAllQueries();
			EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow3);
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
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		
		String dbName = "dataset19";
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
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
		double avgDistance = 4.945309816428576E-4;
		
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(avgDistance, 0d, 1, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		int numOfNewQueries = 3;
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(schemaMap, allPossibleSqlQueries, option, numOfNewQueries);
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
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, 0d, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}

	public static void unitTest4() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String dbName = "dataset19";
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "test_log_file_for_dataset19_1.txt";
		int maxQueriesPerWindow = 1000;
		List<String> allPossibleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		System.out.println("Initializing workload generator...");
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		int numOfNewQueries = 3;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_dataset19.txt";
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.0003, 0d, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		System.out.println(window1);
		System.out.println("Start to forcast next workload...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, 0d, option).distance(qlist1, qlistWindow2);
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
			e.printStackTrace();
		}
	}

	@Override
	public DistributionDistanceGenerator<EuclideanDistance> getDistributionDistanceGenerator()
			throws Exception {
		return new EuclideanDistanceWithSimpleUnion.Generator(schema, penaltyForGoingFromZeroToNonZero, whichClauses);
	}

}


