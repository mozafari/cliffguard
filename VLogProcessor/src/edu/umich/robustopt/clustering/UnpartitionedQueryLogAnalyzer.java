package edu.umich.robustopt.clustering;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryTemporalComparator;
import edu.umich.robustopt.clustering.QueryWindow;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistance_ClusterFrequency;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.SimpleTPCHSyntheticWorkloadGenerator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

public class UnpartitionedQueryLogAnalyzer<Q extends Query> extends QueryLogAnalyzer {
	private List<Q> all_queries = null;
	
	public UnpartitionedQueryLogAnalyzer(QueryParser<Q> qParser, List<Q> all_queries, DistributionDistanceGenerator<? extends DistributionDistance> distanceGenerator, String logFile) throws Exception {
		super(qParser, distanceGenerator, LogLevel.DEBUG, logFile);
		this.all_queries = all_queries;
	}

	public UnpartitionedQueryLogAnalyzer(QueryParser<Q> qParser, List<Q> all_queries, DistributionDistanceGenerator<? extends DistributionDistance> distanceGenerator) throws Exception {
		this(qParser, all_queries, distanceGenerator, null);
	}

	public List<Q> getAll_queries() throws CloneNotSupportedException {
		return Collections.unmodifiableList(all_queries);
	}

	
	public void sanityCheck() throws Exception {
		for (int windowSizeInDays=7; windowSizeInDays<=30; windowSizeInDays+=7) {
			List<QueryWindow> windows = splitIntoTimeEqualWindows(windowSizeInDays);
			System.out.println("window size="+windowSizeInDays+" days, #of windows="+ windows.size());
			for (int winId=0; winId < windows.size(); ++winId) {
				QueryWindow win = windows.get(winId);
				if (!win.getQueries().isEmpty())
					System.out.println("win" + winId +" spans " + win.getQueries().get(0).getTimestamp() + " to " + win.getQueries().get(win.getQueries().size()-1).getTimestamp());
				else
					System.out.println("win" + winId + " is empty");
			}
			measureAvgDistanceBetweenConsecutiveWindows(windows);
			System.out.println("==============================");
		}		
	}

	public List<QueryWindow> splitIntoEqualNumberOfQueries(int windowSizeInNumberOfQueries, boolean sortBasedOnTimestamp) throws Exception {		
		return splitIntoEqualNumberOfQueries(getAll_queries(), windowSizeInNumberOfQueries, sortBasedOnTimestamp);
	}
	
	public List<QueryWindow> splitIntoTimeEqualWindows(int windowSizeInDays) throws Exception {		
		return splitIntoTimeEqualWindows(getAll_queries(), windowSizeInDays);
	}
	
	public void measureWindowSize_Lag_AvgDistance(String whereToSaveFiles) throws Exception {
		measureWindowSize_Lag_AvgDistance(getAll_queries(), whereToSaveFiles);
	}
	
	public void measureWindowSize_AvgConsecutiveDistance(String whereToSaveFiles) throws Exception {
		measureWindowSize_AvgConsecutiveDistance(getAll_queries(), whereToSaveFiles);
	}
	
	public void measureWindowSize_WindowId_ConsecutiveDistance(String whereToSaveFile) throws Exception {
		measureWindowSize_WindowId_ConsecutiveDistance(getAll_queries(), whereToSaveFile);
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

		PartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new PartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), windowsQueries, new DistributionDistance_ClusterFrequency.Generator());
		DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.getAll_windows());
		
		System.out.println("====================\nAvg Distance between consecutive windows");
		System.out.println(avgDist.showSummary());

	}
	
	public static void main(String args[]) {
		try {
			String configFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf";
			List<DatabaseLoginConfiguration> allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(configFile, VerticaDatabaseLoginConfiguration.class.getSimpleName());
			
			String DBalias = "dataset19";
			
			Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMap(DBalias, allDatabaseConfigurations).getSchemas();

			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);

			String topDir = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/" + DBalias + "/";
			//String logFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/" + DBalias + "/parsed.timestamped";
			String logFile = topDir + "parsed-runnable-improvable.timestamped";
			
			List<Query_SWGO> windowsQueriesSWGO = sqlLogFileManager.loadTimestampQueriesFromFile(logFile );
														
			// using DistributionDistance_ClusterFrequency as DistributionDistance
			//UnpartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), new DistributionDistance_ClusterFrequency.Generator());
			// using DistributionDistancePair as distance
			//UnpartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), new DistributionDistancePair.Generator());
			// using EuclideanDistance as distance
			double penalty = 1.0;
			Set<UnionOption> option = new HashSet<UnionOption> (){{  
		           add(UnionOption.SELECT);  
		           add(UnionOption.WHERE);  
		           add(UnionOption.GROUP_BY);
		           add(UnionOption.ORDER_BY);
			}};
			UnpartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), 
					new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option));

			if (args.length == 0) {
				analyzer.measureWindowSize_Lag_AvgDistance("/tmp/WindowSize_Lag_AvgDistance.txt");
				analyzer.measureWindowSize_AvgConsecutiveDistance("/tmp/WindowSize_AvgConsecutiveDistance.txt");
				analyzer.measureWindowSize_WindowId_ConsecutiveDistance("/tmp/WindowSize_WindowId_ConsecutiveDistance.txt");
				
				int windowSizeInDays = 7;
				DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.splitIntoTimeEqualWindows(7));
				
				System.out.println("====================\nAvg Distance between consecutive windows");
				System.out.println(avgDist.showSummary());
				
			} else if (args.length == 3) {
				int numberOfDaysInEachWindow = Integer.parseInt(args[0]);
				int numberOFInitialWindowsToSkip = Integer.parseInt(args[1]);
				int numberOfWindowsToRead = Integer.parseInt(args[2]);
				if (numberOfDaysInEachWindow<1 || numberOFInitialWindowsToSkip <0 || (numberOfWindowsToRead<1 && numberOfWindowsToRead!=-1))
					throw new Exception("Invalid arguments: " + "numberOfDaysInEachWindow=" + numberOfDaysInEachWindow + 
							", numberOFInitialWindowsToSkip=" + numberOFInitialWindowsToSkip + ", numberOfWindowsToRead=" + numberOfWindowsToRead);	
				
				List<QueryWindow> windows = analyzer.splitIntoTimeEqualWindows(numberOfDaysInEachWindow);
				numberOfWindowsToRead = (numberOfWindowsToRead==-1 ? windows.size() - numberOFInitialWindowsToSkip : numberOfWindowsToRead);
				windows = windows.subList(numberOFInitialWindowsToSkip, numberOFInitialWindowsToSkip+numberOfWindowsToRead);
				DistributionDistance dist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(windows);
				String dirPath = topDir + "dvals/" + dist.toString() + "/";
				File directory = new File(dirPath);
				if (directory.exists())
					throw new Exception("Directory already exists: " + dirPath);
				else
					directory.mkdir();
				SqlLogFileManager.writeListOfQueryWindowsToSeparateFiles(directory, windows);
			} else 
				System.err.println("Usage: UnpartitionedQueryLogAnalyzer [numberOfDaysInEachWindow   numberOFInitialWindowsToSkip    numberOfWindowsToRead]");
			
			System.out.println("Done.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
