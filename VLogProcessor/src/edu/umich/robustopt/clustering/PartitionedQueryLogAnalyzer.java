package edu.umich.robustopt.clustering;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.algorithms.FutureKnowingNominalDesignAlgorithm;
import edu.umich.robustopt.algorithms.NoDesigner;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryTemporalComparator;
import edu.umich.robustopt.clustering.QueryWindow;
import edu.umich.robustopt.clustering.Query_SWGO.QParser;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.experiments.AlgorithmEvaluation;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistance_ClusterFrequency;
import edu.umich.robustopt.workloads.SimpleTPCHSyntheticWorkloadGenerator;

public class PartitionedQueryLogAnalyzer<Q extends Query> extends QueryLogAnalyzer {
	// private List<Query> all_queries = null;
	private List<QueryWindow> all_windows = null;

	public PartitionedQueryLogAnalyzer(QueryParser<Q> qParser, List<List<Query>> all_windows, DistributionDistanceGenerator<? extends DistributionDistance> distanceGenerator) throws Exception {
		super(qParser, distanceGenerator, LogLevel.DEBUG);
		
		if (all_windows != null) {
			this.all_windows = new ArrayList<QueryWindow>();
			for (List<Query> ql : all_windows)
				this.all_windows.add(new QueryWindow(ql));
		}
	}


	/*
	public List<Query> getAll_queries() throws CloneNotSupportedException {
		return Collections.unmodifiableList(all_queries);
	}
	*/
	
	public List<QueryWindow> getAll_windows() throws CloneNotSupportedException {
		return Collections.unmodifiableList(all_windows);
	}

	public DistributionDistance measureAvgDistanceBetweenConsecutiveWindows() throws Exception {
		return measureAvgDistanceBetweenConsecutiveWindows(getAll_windows());
	}


	
	/*
	public void sanityCheck() throws Exception {
		for (int windowSizeInDays=7; windowSizeInDays<=30; windowSizeInDays+=7) {
			List<QueryWindow> windows = splitIntoWindows(getAll_queries(), windowSizeInDays);
			System.out.println("window size="+windowSizeInDays+" days, #of windows="+ windows.size());
			measureAvgDistanceBetweenConsecutiveWindows(windows);
			System.out.println("==============================");
		}		
	}
	*/
	
	private List<Query> appendAllWindows () throws CloneNotSupportedException{
		List<Query> all_queries = new ArrayList<Query>();
		// for (int i=0; i<all_queries.size(); ++i)
		//	this.all_queries.add(all_queries.get(i).clone());
		
		if (all_windows != null)
			for (QueryWindow ql : all_windows)
				for (Query q : ql.getQueries())
					all_queries.add(q.clone());
		
		return all_queries;
	}
	
	public static void unitTest1() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("tpch", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();

		List<List<Query_SWGO>> windowsQueriesSWGO = new ArrayList<List<Query_SWGO>>(); 
		
		String s1 = "SELECT l_shipmode FROM lineitem;";
		String s2 = "SELECT l_partkey FROM lineitem;";
		String s3 = "SELECT l_comment FROM lineitem;";
		String s4 = "SELECT l_returnflag FROM lineitem;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		windowsQueriesSWGO.add(wq1);
		List<String> w2 = new ArrayList<String>();
		w2.add(s1); w2.add(s3); w2.add(s3);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		windowsQueriesSWGO.add(wq2);
		
		List<List<Query>> windowsQueries = new ArrayList<List<Query>>(); 
		
		for (int i=0; i<windowsQueriesSWGO.size(); ++i) {
			List<Query> thisWindow = new ArrayList<Query>();
			for (int j=0; j<windowsQueriesSWGO.get(i).size(); ++j)
				thisWindow .add(windowsQueriesSWGO.get(i).get(j));
			windowsQueries.add(thisWindow);
		}
		PartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new PartitionedQueryLogAnalyzer(new Query_SWGO.QParser(), windowsQueries, new DistributionDistance_ClusterFrequency.Generator());
		DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.getAll_windows());
		
		System.out.println("====================\nAvg Distance between consecutive windows");
		System.out.println(avgDist.showSummary());

	}

	private static void filterQueriesBasedOnTheirImprovability(String evaluationFileName, String baselineAlgName, String idealAlgName, String outputDirectoryName, double minAcceptableImprovementRatio) throws IOException {
		try {			
			AlgorithmEvaluation algEval = AlgorithmEvaluation.loadEvaluationFromFile(evaluationFileName);
			
			Map<DistributionDistance, List<List<PerformanceRecord>>> dvalsWindowsQueriesPerformance = algEval.getFinalDvalsWindowsQueryPerformance();
			int num_d_values = dvalsWindowsQueriesPerformance.size();
			assert (num_d_values>0);
						
			DistributionDistance[] dvalues = dvalsWindowsQueriesPerformance.keySet().toArray(new DistributionDistance[1]);
			Arrays.sort(dvalues);
			int totalSkipped = 0;
			int totalQualified = 0;
			int dIdx = 0;
			for (DistributionDistance d : dvalues) { // for each d value
				String dirName = outputDirectoryName + "d" + (dIdx++) + "-" + d.toString() + "/";
				File directory = new File(dirName);
				if (!directory.mkdir())
					throw new Exception("Could not create directory " + dirName);
				for (int w=1; w <dvalsWindowsQueriesPerformance.get(d).size(); ++w) { // for each window
					String fileName = dirName + "w" + w + ".queries";
					BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(fileName)));

					int thisWindowSkipped = 0;
					int thisWindowQualified = 0;

					for (int q=0; q < dvalsWindowsQueriesPerformance.get(d).get(w).size(); ++q) { // for each query
						boolean improvable = false;
						PerformanceRecord performanceRecord = dvalsWindowsQueriesPerformance.get(d).get(w).get(q);
						long baselineLatency = performanceRecord.getPerformanceValueWithDesign(baselineAlgName).getMeanActualLatency();
						long idealLatency = performanceRecord.getPerformanceValueWithDesign(idealAlgName).getMeanActualLatency();
						if (baselineLatency == Long.MAX_VALUE && idealLatency < Long.MAX_VALUE)
							improvable = true;
						
						if (baselineLatency < Long.MAX_VALUE && idealLatency < Long.MAX_VALUE)
							if (baselineLatency / idealLatency >= minAcceptableImprovementRatio)
								improvable = true;
						
						if (improvable) {
							bufferedWriter.append(performanceRecord.getQuery() + "\n");
							totalQualified++;
							thisWindowQualified++;
						} else {
							totalSkipped++;
							thisWindowSkipped++;
						}
					} // q
					
					System.out.println("win="+w + ": qualified queries=" + thisWindowQualified + ", skipped queries=" + thisWindowSkipped);
					bufferedWriter.close();
				} // w
			} // d
			System.out.println("Total qualified queries=" + totalQualified + ", total skipped queries=" + totalSkipped);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
	public static void main(String args[]) {
		try {
			
			String tempDirName = "/Users/sina/robust-opt/processed_workloads/real/dataset19/dvals/";
			String evaluationFileName = tempDirName + "least_to_best_results-1.3400905611498828_0.87152944539737-max_windows-12-max_queries_per_window--1.results";
			String outputDirectoryName = tempDirName + "filtered/";
			filterQueriesBasedOnTheirImprovability(evaluationFileName, NoDesigner.class.getSimpleName(), FutureKnowingNominalDesignAlgorithm.class.getSimpleName(), outputDirectoryName, 2.0);
			
			if (1==1)
				return;
			
			String dirName = "/Users/sina/robust-opt/dataset_synthetic_tpch/d2-0.1_0.7/";
			int numberOfWindows = 10;
			
			Map<String, Schema> schemaMap;
			String DBalias = "tpch";
			if (DBalias.equals("tpch"))
				schemaMap = SchemaUtils.GetTPCHSchema();
			else
				schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(DBalias, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
	
			List<List<Query_SWGO>> windowsQueriesSWGO = new ArrayList<List<Query_SWGO>>();
			
			List<List<String>> sqlWindows = SqlLogFileManager.loadWindowsOfQueryStringsFromSeparateFiles(dirName, numberOfWindows, -1);
			windowsQueriesSWGO = new Query_SWGO.QParser().convertSqlListOfListToQuery(sqlWindows, schemaMap);
			
			List<List<Query>> windowsQueries = new ArrayList<List<Query>>(); 
			for (int i=0; i<windowsQueriesSWGO.size(); ++i) {
				List<Query> thisWindow = new ArrayList<Query>();
				for (int j=0; j<windowsQueriesSWGO.get(i).size(); ++j)
					thisWindow .add(windowsQueriesSWGO.get(i).get(j));
				windowsQueries.add(thisWindow);
			}
											
			PartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new PartitionedQueryLogAnalyzer(new Query_SWGO.QParser(), windowsQueries, new DistributionDistance_ClusterFrequency.Generator());
			DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.getAll_windows());
			
			System.out.println("====================\nAvg Distance between consecutive windows");
			System.out.println(avgDist.showSummary());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
