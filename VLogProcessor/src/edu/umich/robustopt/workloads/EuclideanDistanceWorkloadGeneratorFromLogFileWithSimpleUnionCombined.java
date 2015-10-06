package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

public class EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined 
	extends LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses {
	public final static Set<UnionOption> AllClausesOption = new HashSet<UnionOption> (){{  
        add(UnionOption.SELECT);  
        add(UnionOption.WHERE);  
        add(UnionOption.GROUP_BY);
        add(UnionOption.ORDER_BY);
	}};

	private List<String> exampleSqlQueries = null;
	private int numOfNewQueries;
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(
			Map<String, Schema> schema, List<String> exampleSqlQueries, Set<UnionOption> option, int numOfNewQueries) throws Exception {
		super(schema, exampleSqlQueries, option);
		this.exampleSqlQueries = exampleSqlQueries;
		this.numOfNewQueries = numOfNewQueries;
	}
	
	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(
			String dbAlias, String DBVendor, List<String> exampleSqlQueries, Set<UnionOption> option, int numOfNewQueries) throws Exception {
		super(dbAlias, DBVendor, exampleSqlQueries, option);
		this.exampleSqlQueries = exampleSqlQueries;
		this.numOfNewQueries = numOfNewQueries;
	}

	public EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(
			String dbAlias, List<DatabaseLoginConfiguration> allDatabaseConfigurations,
			List<String> exampleSqlQueries, Set<UnionOption> option, int numOfNewQueries) throws Exception {
		super(dbAlias, allDatabaseConfigurations, exampleSqlQueries, option);
		if (option.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.numOfNewQueries = numOfNewQueries;

	}

	@Override
	public ClusteredWindow forecastNextWindow(ClusteredWindow originalWindow,
			EuclideanDistance distance) throws Exception {
		if (distance == null) {
			throw new Exception("You need to provide a EuclideanDistance");
		}
		penaltyForGoingFromZeroToNonZero = distance.getPenaltyForGoingFromZeroToNonZero();
		if (originalWindow == null) {
			throw new Exception("We cannot forecast the next window from a null window!");
		}
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui ed_DimReduce = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui(schema, exampleSqlQueries, whichClauses, numOfNewQueries);
		ClusteredWindow nextWindow = ed_DimReduce.forecastNextWindow(originalWindow, distance);
		if (nextWindow == null) {
			EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong ed = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong(schema, exampleSqlQueries, whichClauses);
			nextWindow = ed.forecastNextWindow(originalWindow, distance);
		}
		return nextWindow;
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
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.04);
		int numOfNewQueries = 1;
		//got everything set up
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistance distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is "+distverify);
		System.out.println(window2);
	}
	
	public static void unitTest2() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/dataset_synthetic_tpch/"; 
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String dbAlias = "dataset19";
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
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
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
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow2);
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
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.02);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");   
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlistWindow2);
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
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w0.queries";
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<Query_SWGO> allPossibleQueries = sqlLogFileManager.loadTimestampQueriesFromFile(unionSqlQueriesFile);
		List<String> allPossibleSqlQueries = new ArrayList<String>();
		for (Query_SWGO q : allPossibleQueries)
			allPossibleSqlQueries.add(q.getSql());
				
		List<String> wq1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, 10000);
		List<Query_SWGO> swgo_qlist1 = new Query_SWGO.QParser().convertSqlListToQuery(wq1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(swgo_qlist1);
		double avgDistance = 4.945309816428576E-4;
		
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(avgDistance);
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(schemaMap, allPossibleSqlQueries, option, numOfNewQueries);
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
		}};
		int numOfNewQueries = 1;
		EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined workloadgenerator = new EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionCombined(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName(), allPossibleQueries, option, numOfNewQueries);
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/DBD-parser/" + "big_window_for_dataset19.txt";
		List<String> w1 = SqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		EuclideanDistanceWithSimpleUnion dist1 = new EuclideanDistanceWithSimpleUnion(0.0003);
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
	
	public static void main(String args[]) {
		try {
//			unitTest35();
//			unitTest1();
//			unitTest2();
//			unitTest3();
			unitTest4();
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