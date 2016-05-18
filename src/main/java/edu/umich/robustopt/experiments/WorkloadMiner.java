package edu.umich.robustopt.experiments;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
import edu.umich.robustopt.staticanalysis.SQLQueryAnalyzer;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;

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

	public static void deriveInsight (Map<String, Schema> schemaMap, String outputDirectory, String inputTimestampedQueryLogFile,
								int numberOfDaysInEachWindow, int numberOFInitialWindowsToSkip, int numberOfWindowsToRead) throws Exception {
		if (numberOfDaysInEachWindow<1 || numberOFInitialWindowsToSkip <0 || (numberOfWindowsToRead<1 && numberOfWindowsToRead!=-1))
			throw new Exception("Invalid arguments: " + "numberOfDaysInEachWindow=" + numberOfDaysInEachWindow + 
					", numberOFInitialWindowsToSkip=" + numberOFInitialWindowsToSkip + ", numberOfWindowsToRead=" + numberOfWindowsToRead);	

		try {
			//List<DatabaseLoginConfiguration> allDatabaseConfigurations = loadDatabaseLoginConfigurations(db_vendor, loginConfigFile);
			//Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMap(DBalias, allDatabaseConfigurations).getSchemas();

			//System.out.println("=======================================================================");
			if (schemaOut) {
				System.out.println();
				System.out.println("Current Schema: ");
				System.out.println(schemaMap.get(SchemaUtils.defaultSchemaName));
			}
			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
			List<Query_SWGO> windowsQueriesSWGO = sqlLogFileManager.loadTimestampQueriesFromFile(inputTimestampedQueryLogFile);
			//System.out.println("=======================================================================");

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

			// All the nonexisting parent directory should be created ahead of time.
			new File(outputDirectory).mkdirs();
			String logFile = outputDirectory + File.separator + "UnpartitionedQueryLogAnalyzer.log";
			UnpartitionedQueryLogAnalyzer<Query_SWGO> analyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), 
					new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option), logFile);

			analyzer.measureWindowSize_Lag_AvgDistance(outputDirectory + File.separatorChar + "WindowSize_Lag_AvgDistance.txt");
			analyzer.measureWindowSize_AvgConsecutiveDistance(outputDirectory + File.separatorChar + "WindowSize_AvgConsecutiveDistance.txt");
			analyzer.measureWindowSize_WindowId_ConsecutiveDistance(outputDirectory + File.separatorChar + "WindowSize_WindowId_ConsecutiveDistance.txt");
				
			DistributionDistance avgDist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(analyzer.splitIntoTimeEqualWindows(numberOfDaysInEachWindow));

			if (allOut) {
				System.out.println("Avg Distance between consecutive windows, each window " + numberOfDaysInEachWindow + " days long");
				System.out.println(avgDist.showSummary());
			}
			List<QueryWindow> windows = analyzer.splitIntoTimeEqualWindows(numberOfDaysInEachWindow);
			numberOfWindowsToRead = (numberOfWindowsToRead==-1 ? windows.size() - numberOFInitialWindowsToSkip : numberOfWindowsToRead);
			windows = windows.subList(numberOFInitialWindowsToSkip, numberOFInitialWindowsToSkip+numberOfWindowsToRead);
			DistributionDistance dist = analyzer.measureAvgDistanceBetweenConsecutiveWindows(windows);
				
			String dirPath = outputDirectory + File.separatorChar + "separateWindows-" + numberOfDaysInEachWindow + "daysEach";
			File directory = new File(dirPath);
			if (directory.exists()) {
				//throw new Exception("Directory already exists: " + dirPath);
			}
			else
				directory.mkdir();
			//SqlLogFileManager.writeListOfQueryWindowsToSeparateFiles(directory, windows);

			System.out.println("Done mining your past workload.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void _main(String[] args) throws Exception {
		//String db_vendor; // vertica or microsoft
		//String dbAlias;
		String outputDirectory;
		String inputTimestampedQueryLogFile;
		String homeDir = System.getProperty("user.home");
		//String database_login_file = homeDir + File.separator + "databases.conf";
		int numberOfDaysInEachWindow = 7;
		int numberOFInitialWindowsToSkip = 0;
		int numberOfWindowsToRead = -1;

		String usageMessage = "Usage: java -cp CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner schema_file query_file output_dir output_dir [window_size_in_days number_of_initial_windows_to_skip number_of_windows_to_read]"
				+ "\n\n"
				//+ "db_vendor: either 'vertica' or 'microsoft' (without quotations)\n"
				//+ "db_alias: the short name of the database (e.g., tpch, employmentInfo). This is the ID associated to the target database in the db_login_file. In other words, the db_alias is used to find the appropriate login information from db_login_file\n"
				//+ "db_login_file: an xml file with the login information (see databases.conf as an example)\n"
				+ "schema_file: a file describing the schema of the parsed query file in form of data definition language(DDL).\n"
				+ "query_file: an CSV file (using | as separators) with timestamp followed by a single query in each line.\n"
				+ "\tFor example: \n"
				+ "\t2011-12-14 19:38:51|select avg(salary) from employee\n"
				+ "\t2011-12-14 19:38:51|commit\n\n"
				+ "output_dir: an empty directory to store the output of the analysis.\n"
				+ "window_size_in_days: number of days in each window, as a unit of anlysis (e.g., 7 for analyzing weekly patterns and 30 for analyzing monthly patterns). Default: 7.\n"
				+ "number_of_initial_windows_to_skip: to account for early stages of the database lifetime when few query had run. Default:0 .\n"
				+ "number_of_windows_to_read: to control the total number windows to analyze (choose -1 to read all windows of queries). Default: -1.\n";
		

		
		if (args.length !=6 && args.length!=3) {
			log.error(usageMessage);
			return;
		} 
		// we have the right number of parameters
		int idx = 0;
		//db_vendor = args[idx++]; db_vendor = db_vendor.toLowerCase();
		String schemaFileName = args[idx++];
		//assert db_vendor.equals("vertica") || db_vendor.equals("microsoft");
		//dbAlias = args[idx++];
		//database_login_file = args[idx++];
		inputTimestampedQueryLogFile = args[idx++];
		outputDirectory = args[idx++];
		if (args.length == 6) {
			numberOfDaysInEachWindow = Integer.parseInt(args[idx++]);
			assert numberOfDaysInEachWindow>0;
			numberOFInitialWindowsToSkip  = Integer.parseInt(args[idx++]);
			assert numberOFInitialWindowsToSkip >=0;
			numberOfWindowsToRead  = Integer.parseInt(args[idx++]);
			assert numberOfWindowsToRead > 0 || numberOfWindowsToRead==-1;
		}
		/*
		log.status(LogLevel.STATUS, "Running with the following parameters:\n"
				//+ "db_vendor=" + db_vendor
				//+ "\ndb_alias=" + dbAlias
				//+ "\ndb_login_file="+ database_login_file
				+ "\nschema_file = " + schemaFileName
				+ "\nquery_file = " + inputTimestampedQueryLogFile
				+ "\noutput_dir = " + outputDirectory
				+ "\nwindow_size_in_days = " + numberOfDaysInEachWindow
				+ "\nnumber_of_initial_windows_to_skip = " + numberOFInitialWindowsToSkip
				+ "\nnumber_of_windows_to_read = " + numberOfWindowsToRead
				+ "\n"
				);
	*/
		
		Timer t = new Timer();
		File schemaFile = new File(schemaFileName);
		deriveInsight(SchemaUtils.GetSchemaMap(schemaFile).getSchemas(), outputDirectory, inputTimestampedQueryLogFile, numberOfDaysInEachWindow, numberOFInitialWindowsToSkip, numberOfWindowsToRead);
		System.out.println("Mining your workload took " + t.lapMinutes() + " minutes!");
		
		log.status(LogLevel.STATUS, "DONE.");
		
	}

	static public void print_help() {
		System.out.println(
				"Usage:\n" +
				"  java -cp CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner schema_file query_file [options]\n" +
				"\n" +
				"Options:\n" +
				"  -g --general      General statistics on appearing times of given tables and columns\n" +
				"  -p --popularity   Popular tables/columns in terms of number of queries in which which they appear\n" +
				"  -c --combination  Popular combinations of columns in terms of number of queries in which they appear together\n" +
				"  -j --join         Popular joined column groups\n" +
				"  -a --aggregate    Statistics about columns that have appeared as a parameter to aggregate functions\n" +
				"  --all             Output all the mining results\n" +
				"  --help            Help for WorkloadMiner\n" +
				"  --only-column     Only show results about COLUMN statistics\n" +
				"  --only-table      Only show results about TABLE statistics\n" +
				"  --show-schema     Show the schema that the query file is using\n");
	}

	static private boolean schemaOut = false;
	static private boolean allOut = false;
	static public void main(String[] args) throws Exception {
		OptionParser parser = new OptionParser( "g::p::c::j::a::" );
		parser.accepts("general").withOptionalArg();
		parser.accepts("popularity").withOptionalArg();
		parser.accepts("combination").withOptionalArg();
		parser.accepts("join").withOptionalArg();
		parser.accepts("aggregate").withOptionalArg();
		parser.accepts("all");
		parser.accepts("help");
		parser.accepts("only-columns");
		parser.accepts("only-tables");

		if (args.length<2) {
			OptionSet options = parser.parse(args);
			if (options.has("help"))
				print_help();
			else print_help();
			return;
		}

		File schemaFile = new File(args[0]);
		File queryFile = new File(args[1]);

		if (!schemaFile.exists() || schemaFile.isDirectory())
			System.out.println("Schema file: " + schemaFile.toString() + " does not exist! Exit.");
		if (!queryFile.exists() || queryFile.isDirectory())
			System.out.println("Query file: " + queryFile.toString() + " does not exist! Exit.");
		OptionSet options = parser.parse(Arrays.copyOfRange(args, 2, args.length));

		SQLQueryAnalyzer.Configuration config = new SQLQueryAnalyzer.Configuration();
		String gOpt = null, pOpt = null, jOpt = null, aOpt = null, cOpt = null;
		if (options.has("g")) gOpt = "g";
		else if (options.has("general")) gOpt = "general";
		if (options.has("p")) pOpt = "p";
		else if (options.has("popularity")) pOpt = "popularity";
		if (options.has("j")) jOpt = "j";
		else if (options.has("join")) jOpt = "join";
		if (options.has("a")) aOpt = "a";
		else if (options.has("aggregate")) aOpt = "aggregate";
		if (options.has("c")) cOpt = "c";
		else if (options.has("combination")) cOpt = "combination";
		if (gOpt!=null) {
			if (options.hasArgument(gOpt))
				config.g_mode = options.valueOf(gOpt).toString();
			else
				config.g_mode = "u";
		}
		if (pOpt!=null) {
			if (options.hasArgument(pOpt))
				config.p_mode = options.valueOf(pOpt).toString();
			else
				config.p_mode = "u";
		}
		if (jOpt!=null) {
			if (options.hasArgument(jOpt))
				config.j_mode = options.valueOf(jOpt).toString();
			else
				config.j_mode = "u";
		}
		if (aOpt!=null) {
			if (options.hasArgument(aOpt))
				config.a_mode = options.valueOf(aOpt).toString();
			else
				config.a_mode = "u";
		}
		if (cOpt!=null) {
			if (options.hasArgument(cOpt))
				config.c_mode = options.valueOf(cOpt).toString();
			else
				config.c_mode = "u";
		}


		if (options.has("all")) { // u = union
			config.g_mode = "uswfgo";
			config.p_mode = "uswfgo";
			config.c_mode = "uswfgo";
			config.j_mode = "ug";
			config.a_mode = "ug";
			allOut = true;
		}

		if (options.has("only-columns")) {
			if (options.has("only-tables")) {
				System.out.println("--only-columns option conflict with --only-tables");
				print_help();
			}
			else config.table_on = false;
		}
		else if (options.has("only-tables"))
			config.column_on = false;

		if (options.has("show-schema"))
			schemaOut = true;

		if (args.length==2) {
			config.g_mode = "u";
			config.p_mode = "u";
			config.c_mode = "u";
			config.j_mode = "u";
			config.a_mode = "u";
		}

		if (options.has("help")) {
			print_help();
			return;
		}

		SQLQueryAnalyzer.setConfig(config);
		Path tmp = Files.createTempDirectory(null);
		FileUtils.forceDeleteOnExit(tmp.toFile());
		_main(new String[]{schemaFile.toString(), queryFile.toString(), tmp.toString()});
	}

}
