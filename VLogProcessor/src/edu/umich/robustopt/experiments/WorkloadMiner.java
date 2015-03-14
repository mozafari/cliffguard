package edu.umich.robustopt.experiments;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.algorithms.RobustDesigner;
import edu.umich.robustopt.clustering.QueryWindow;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.clustering.UnpartitionedQueryLogAnalyzer;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

public class WorkloadMiner {
	
	public enum DistanceChoiceMechanism {
		ALL_FUTURE, // use all the distances d1, ..., dn to compute the distance for window d_i
		ONE_FUTURE, // use the exact window itself, i.e., use di for di
		ALL_PAST, // use all the distances d1, ..., d(i_1) to compute the distance for window d_i
		ONE_PAST, // use the past window's distance for this window, i.e., d_(i-1) for di
		FIXED // use the same given fixed value
	}
	
	static BLog log = new BLog(LogLevel.VERBOSE);
				
	public static List<String> convertListOfListsIntoList(List<List<String>> twoDimentionalArray) throws Exception {
		if (twoDimentionalArray==null || twoDimentionalArray.isEmpty())
			throw new Exception("You need to provide a non-empty list of lists..");
		List<String> objList = new ArrayList<String>();
		for (List<String> list : twoDimentionalArray)
			objList.addAll(list);
		return objList;
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

	
	public static List<DatabaseLoginConfiguration> loadDatabaseLoginConfigurations(String db_vendor, String database_login_file) throws Exception {
		String DBVendor;
		List<DatabaseLoginConfiguration> allDatabaseConfigurations;
		if (db_vendor.equalsIgnoreCase("microsoft")) {
			DBVendor = MicrosoftDatabaseLoginConfiguration.class.getSimpleName();
			allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(database_login_file, DBVendor);
		} else if (db_vendor.equalsIgnoreCase("vertica")) {
			DBVendor = VerticaDatabaseLoginConfiguration.class.getSimpleName();
			allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(database_login_file, DBVendor);
		} else
			throw new Exception("Unsupported vendor: " + db_vendor);

		return allDatabaseConfigurations;
	}
	
	public static void deriveInsight (String loginConfigFile, String db_vendor, String DBalias, String outputDirectory, String inputTimestampedQueryLogFile,
								int numberOfDaysInEachWindow, int numberOFInitialWindowsToSkip, int numberOfWindowsToRead) throws Exception {
		if (numberOfDaysInEachWindow<1 || numberOFInitialWindowsToSkip <0 || (numberOfWindowsToRead<1 && numberOfWindowsToRead!=-1))
			throw new Exception("Invalid arguments: " + "numberOfDaysInEachWindow=" + numberOfDaysInEachWindow + 
					", numberOFInitialWindowsToSkip=" + numberOFInitialWindowsToSkip + ", numberOfWindowsToRead=" + numberOfWindowsToRead);	

		try {
			List<DatabaseLoginConfiguration> allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(loginConfigFile, VerticaDatabaseLoginConfiguration.class.getSimpleName());
			Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMap(DBalias, allDatabaseConfigurations).getSchemas();

			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
			
			List<Query_SWGO> windowsQueriesSWGO = sqlLogFileManager.loadTimestampQueriesFromFile(inputTimestampedQueryLogFile);
														
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
			String logFile = outputDirectory + File.separator + "UnpartitionedQueryLogAnalyzer.log";
			UnpartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), 
					new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option), logFile);

			analyzer.measureWindowSize_Lag_AvgDistance(outputDirectory + File.separatorChar + "WindowSize_Lag_AvgDistance.txt");
			analyzer.measureWindowSize_AvgConsecutiveDistance(outputDirectory + File.separatorChar + "WindowSize_AvgConsecutiveDistance.txt");
			analyzer.measureWindowSize_WindowId_ConsecutiveDistance(outputDirectory + File.separatorChar + "WindowSize_WindowId_ConsecutiveDistance.txt");
				
			DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.splitIntoTimeEqualWindows(numberOfDaysInEachWindow));
				
			System.out.println("====================\nAvg Distance between consecutive windows, each window " + numberOfDaysInEachWindow + " days long");
			System.out.println(avgDist.showSummary());
				
			List<QueryWindow> windows = analyzer.splitIntoTimeEqualWindows(numberOfDaysInEachWindow);
			numberOfWindowsToRead = (numberOfWindowsToRead==-1 ? windows.size() - numberOFInitialWindowsToSkip : numberOfWindowsToRead);
			windows = windows.subList(numberOFInitialWindowsToSkip, numberOFInitialWindowsToSkip+numberOfWindowsToRead);
			DistributionDistance dist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(windows);
				
			String dirPath = outputDirectory + File.separatorChar + "separateWindows-" + numberOfDaysInEachWindow + "daysEach";
			File directory = new File(dirPath);
			if (directory.exists())
				throw new Exception("Directory already exists: " + dirPath);
			else
				directory.mkdir();
			SqlLogFileManager.writeListOfQueryWindowsToSeparateFiles(directory, windows);
			System.out.println("Done mining your past workload.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String db_vendor; // vertica or microsoft
		String dbAlias;
		String outputDirectory;
		String inputTimestampedQueryLogFile;
		String homeDir = System.getProperty("user.home");
		String database_login_file = homeDir + File.separator + "databases.conf"; 
		int numberOfDaysInEachWindow = 7;
		int numberOFInitialWindowsToSkip = 0;
		int numberOfWindowsToRead = -1;
		
		
		String usageMessage = "Usage: java -cp CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner db_vendor db_name db_login_file query_file output_dir output_dir [window_size_in_days number_of_initial_windows_to_skip number_of_windows_to_read]"
				+ "\n\n"
				+ "db_vendor: either 'vertica' or 'microsoft' (without quotations)\n"
				+ "db_alias: the short name of the database (e.g., tpch, employmentInfo). This is the ID associated to the target database in the db_login_file. In other words, the db_alias is used to find the appropriate login information from db_login_file\n"
				+ "db_login_file: an xml file with the login information (see databases.conf as an example)\n"
				+ "query_file: an CSV file (using | as separators) with timestamp followed by a single query in each line\n"
				+ "\tFor example: \n"
				+ "\t2011-12-14 19:38:51|select avg(salary) from employee\n"
				+ "\t2011-12-14 19:38:51|commit\n\n"
				+ "output_dir: an empty directory to store the output of the analysis\n"
				+ "window_size_in_days: number of days in each window, as a unit of anlysis (e.g., 7 for analyzing weekly patterns and 30 for analyzing monthly patterns). Default: 7\n"
				+ "number_of_initial_windows_to_skip: to account for early stages of the database lifetime when few query had run. Default:0 \n"
				+ "number_of_windows_to_read: to control the total number windows to analyze (choose -1 to read all windows of queries). Default: -1\n";
		

		
		if (args.length !=8 && args.length!=5) {
			log.error(usageMessage);
			return;
		} 
		// we have the right number of parameters
		int idx = 0;
		db_vendor = args[idx++]; db_vendor = db_vendor.toLowerCase();
		assert db_vendor.equals("vertica") || db_vendor.equals("microsoft");
		dbAlias = args[idx++];
		database_login_file = args[idx++];
		inputTimestampedQueryLogFile = args[idx++];
		outputDirectory = args[idx++];
		if (args.length == 8) {
			numberOfDaysInEachWindow = Integer.parseInt(args[idx++]);
			assert numberOfDaysInEachWindow>0;
			numberOFInitialWindowsToSkip  = Integer.parseInt(args[idx++]);
			assert numberOFInitialWindowsToSkip >=0;
			numberOfWindowsToRead  = Integer.parseInt(args[idx++]);
			assert numberOfWindowsToRead > 0 || numberOfWindowsToRead==-1;
		}
			
		log.status(LogLevel.STATUS, "Running with the following parameters:\n"
				+ "db_vendor=" + db_vendor
				+ "\ndb_alias=" + dbAlias
				+ "\ndb_login_file="+ database_login_file
				+ "\nquery_file="+ inputTimestampedQueryLogFile
				+ "\noutput_dir =" + outputDirectory
				+ "\nwindow_size_in_days=" + numberOfDaysInEachWindow
				+ "\nnumber_of_initial_windows_to_skip=" + numberOFInitialWindowsToSkip
				+ "\nnumber_of_windows_to_read=" + numberOfWindowsToRead
				+ "\n"
				);
	
		
		Timer t = new Timer();
		deriveInsight(database_login_file, db_vendor, dbAlias, outputDirectory, inputTimestampedQueryLogFile, numberOfDaysInEachWindow, numberOFInitialWindowsToSkip, numberOfWindowsToRead);
		System.out.println("Mining your workload took " + t.lapMinutes() + " minutes!");
		
		log.status(LogLevel.STATUS, "DONE.");
		
	}

}
