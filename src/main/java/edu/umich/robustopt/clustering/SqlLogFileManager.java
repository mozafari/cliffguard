package edu.umich.robustopt.clustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.math.stat.StatUtils;



import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;

import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.metering.PerformanceValueWithDesign;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.microsoft.MicrosoftLatencyMeter;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Triple;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaLatencyMeter;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistancePair;

public class SqlLogFileManager<Q extends Query>{
	static BLog log = new BLog(LogLevel.STATUS);
	
	public char fieldSeparator = '|';
	public String querySeparator = "\n";
	public final static String timeStampFormat = "yyyy-MM-dd HH:mm:ss";
	
	QueryParser<Q> qParser;
	private Map<String, Schema> schemaMap = null;
	LatencyMeter latencyMeter = null;
	Boolean useExplainInsteadOfRunningQueries = null;
	DBDesigner dbDesigner = null;
	
	private List<Q> all_queries = null;
	private Long minLatencyInMilliSecs = null;
	private int number_of_all_queries = 0;
	private int number_of_ignored_by_dbd = 0;
	private int number_of_ignored_by_parser = 0;
	private int number_of_ignored_for_latency = 0;
	private int number_of_ignored_for_runtime_error = 0;
	private int number_of_corrupted_queries = 0;
	private int number_of_corrupted_queries_no_delimeter = 0;
	private int number_of_corrupted_queries_timestamp_format = 0;
	private int number_of_empty_queries = 0;

	public SqlLogFileManager(char fieldSeparator, String querySeparator, QueryParser<Q> qParser, Map<String, Schema> schemaMap, LatencyMeter latencyMeter, Boolean useExplainInsteadOfRunningQueries, DBDesigner dbDesigner) {
		this.fieldSeparator = fieldSeparator;
		this.querySeparator = querySeparator;
		this.qParser = qParser;
		this.schemaMap = schemaMap;
		this.latencyMeter = latencyMeter;
		this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
		this.dbDesigner = dbDesigner;
	}
	
	public SqlLogFileManager(char fieldSeparator, String querySeparator, QueryParser<Q> qParser, Map<String, Schema> schemaMap) {
		this(fieldSeparator, querySeparator, qParser, schemaMap, null, null, null);
	}

	public static List<String> loadQueryStringsFromPlainFile(String filename, int maxQueriesPerWindow) throws IOException {
		return loadQueryStringsFromPlainFile(filename, maxQueriesPerWindow, true);
	}
	
	public static List<String> loadQueryStringsFromPlainFile(String filename, int maxQueriesPerWindow, boolean returnOnlyParseableQueries) throws IOException {
		File f = new File(filename);
		List<String> loadedQueriesSql = StringUtils.GetLinesFromFile(f, maxQueriesPerWindow);
		log.status(LogLevel.VERBOSE, "loaded queries from file "+ filename + " with "+  loadedQueriesSql.size() + " queries\n");
		
		List<String> parseableQueries = new ArrayList<String>();
		for (String sqlQuery : loadedQueriesSql) {
			if (returnOnlyParseableQueries)
				try {
					Parser p = new Parser("public", null, sqlQuery);
					parseableQueries.add(sqlQuery);
				} catch (ParseException e) {
					continue;
				}
			else
				parseableQueries.add(sqlQuery);
		}
			
		log.status(LogLevel.VERBOSE, "out of the " + loadedQueriesSql.size() + " queries from file "+ filename + " only "+  parseableQueries.size() + " were parseable\n");
		return parseableQueries;				
	}
	
	// Barzan: added old code from here:

	/*
	public SqlLogManager(String input_query_log, String schemaAlias) throws Exception {
		schemaMap = SchemaUtils.GetSchemaMap(schemaAlias).getSchemas();
		all_queries = load_queries(input_query_log);
	}
	*/

	public void printLogFileStats(){
		System.out.println("number_of_all_queries: "+number_of_all_queries);
		System.out.println("number_of_ignored_by_dbd: "+number_of_ignored_by_dbd);
		System.out.println("number_of_ignored_by_parser: "+number_of_ignored_by_parser);
		System.out.println("number_of_ignored_for_latency: "+number_of_ignored_for_latency + " for a threshold of "+ minLatencyInMilliSecs +" millisecs");
		System.out.println("number_of_ignored_for_runtime_error: "+number_of_ignored_for_runtime_error);
		System.out.println("number_of_corrupted_queries: "+number_of_corrupted_queries);
		System.out.println("number_of_corrupted_queries_no_delimeter: " + number_of_corrupted_queries_no_delimeter);
		System.out.println("number_of_corrupted_queries_timestamp_format: " + number_of_corrupted_queries_timestamp_format);
		System.out.println("number_of_empty_queries: "+ number_of_empty_queries);
	}

	public List<String> loadTimestampQueryStringsFromFile(String input_query_log) throws Exception {
		List<Q> allQueries = loadTimestampQueriesFromFile(input_query_log);
		List<String> allQueryStrings = new ArrayList<String>(); 
		for (Q query : allQueries) {
			allQueryStrings.add(query.getSql());
		}
		
		return allQueryStrings;
	}
		
	/*
	 * Format of each line: 
	 * 2012-03-13 23:47:58.467531-07|select version_internal()
	 * 
	 * Output: <lineNumber, Timestamp, QueryString>
	 */
	public List<Q> loadTimestampQueriesFromFile(String input_query_log) throws Exception {
		if (minLatencyInMilliSecs==null)
			minLatencyInMilliSecs = 100L;
			//findBaselineLatency();
		double latencyFactor = 1.0;

				
		//BufferedReader in = new BufferedReader(new FileReader(input_query_log));
		Scanner in = new Scanner (new File(input_query_log));
		in.useDelimiter(querySeparator);
		
		String line;
		int lineNumber = 0;
		Parser p;
		List<Q> loaded_queries = new ArrayList<Q>();		 
		SimpleDateFormat formatter = new SimpleDateFormat(timeStampFormat);
		 
		while (in.hasNext()) {
			line = in.next();
			++lineNumber;
			log.status(LogLevel.VERBOSE, "" + lineNumber);			
			if (lineNumber % 10000 == 0)
				log.status(LogLevel.STATUS, "" + lineNumber + " lines processed ...");			

			++number_of_all_queries;
			int pos1 = line.indexOf(fieldSeparator);
			//int pos2 = line.indexOf(separator, pos1+1);
			if (pos1 == -1) {
				log.status(LogLevel.STATUS, input_query_log + " lineNumber " + lineNumber + " no separator!");
				++number_of_corrupted_queries;
				++number_of_corrupted_queries_no_delimeter;
				continue;
			}
			String timestampStr = line.substring(0, pos1);
			timestampStr = timestampStr.replaceAll("\n", "");
			timestampStr = timestampStr.replaceAll("\r", "");
			
			//String latencyStr = line.substring(pos1+1, pos2);
			String sql = line.substring(pos1+1);
			  
 			Date timestamp;
			/*
			int pos3 = timestampStr.indexOf('.');
			timestampStr = timestampStr.substring(0, pos3);
			if (pos3 == -1) {
				log.status(LogLevel.STATUS, "Bad timestamp: "+timestampStr);
				throw new ParseException( "Bad timestamp: "+timestampStr);
			}
			*/	
 			try {
 				timestamp = (Date) formatter.parse(timestampStr);
			} catch (java.text.ParseException e) {
				++number_of_corrupted_queries;
				++number_of_corrupted_queries_timestamp_format;
				e.printStackTrace();
				continue;	
		 	}
		 	
			//double latency = Double.parseDouble(latencyStr);
			//if (latency<minLatencyInSecs) {
			//	++number_of_ignored_for_latency;
			//	continue;
			//}
			
 			Q query;
			try {
				query = qParser.parse(lineNumber, timestamp, null, sql, schemaMap); 
				
				if (query.isEmpty()) {
					++ number_of_empty_queries;
				 	continue;
				}
				
				//log.status(LogLevel.VERBOSE, "<line: " + lineNumber + "> " + query.getSql());
				//log.status(LogLevel.VERBOSE, "summarized as: " + query.toString());			 
			} catch (Throwable t) {
				//log.error(input_query_log + " query number: " + lineNumber + "; message: " + t.getMessage());
				++ number_of_ignored_by_parser;
				continue;
			}
			
			if (latencyMeter != null)
				try {
					PerformanceRecord pr = new PerformanceRecord(sql);
					latencyMeter.measureLatency(pr, new ArrayList<PhysicalStructure>(), "test", useExplainInsteadOfRunningQueries);
					PerformanceValueWithDesign pv = pr.getPerformanceValueWithDesign("test");
					long latency = pv.getMeanActualLatency();
					if (latency < latencyFactor * minLatencyInMilliSecs) {
						++ number_of_ignored_for_latency;
						continue;
					}
				} catch (Exception e) {
					log.status(LogLevel.STATUS, "<line: " + lineNumber + "> query: " + sql + " gave the following error: " + e.getMessage());
					++ number_of_ignored_for_runtime_error;
					continue;
				}
			
			if (dbDesigner!=null)
				throw new Exception("We haven't implemented this feature yet!");
			
			loaded_queries.add(query);
			
		}
		in.close();
		
		Collections.sort(loaded_queries, new QueryTemporalComparator());

		all_queries = loaded_queries;
		return loaded_queries;
	}
	
	private List<Q> sortQueriesByTimestamp(List<Q> queriesList) {
		Collections.sort(queriesList, new QueryTemporalComparator());
		return queriesList;
	}
	
	private	List<Q> sortQueriesByTimestamp() {
		return sortQueriesByTimestamp(all_queries);
	}
	
	private void findBaselineLatency() throws Exception {
		if (latencyMeter == null)
			throw new Exception("We cannot measure baseline latency without an actual latency meter!");
		
		int maxNumberOfQueries = 5;
		List<String> queries = new ArrayList<String>();
		queries.add("select 1");
		int i=1;
		for (String schemaName : schemaMap.keySet())
			for (String table : schemaMap.get(schemaName).getAllTables())
				if (i< maxNumberOfQueries) {
					String sql = "select 1 from " + schemaName + "." + table;
					queries.add(sql);
					++i;
				} else 
					break;
		
		int numberOfRepetitions = 3;
		long sumLatency = latencyMeter.measureSumLatency(queries, new ArrayList<PhysicalStructure>(), numberOfRepetitions, useExplainInsteadOfRunningQueries);
		long avgBaselineLatency = sumLatency / queries.size();
		log.status(LogLevel.STATUS, "computed the baseline latency with " + queries.size() + " queries, taking a total of " + sumLatency + " ms and an avergae latency of "+avgBaselineLatency + " ms");

		this.minLatencyInMilliSecs = avgBaselineLatency;
	}

	private static void printLatencyStats(Collection<Query> queries) throws Exception {
		double[] latencies = new double[queries.size()]; 
		int next = 0;
		for (Query q : queries)
			latencies[next++] = q.getOriginalLatency();
		
		log.status(LogLevel.STATUS, "min latency="+StatUtils.min(latencies));
		log.status(LogLevel.STATUS, "mean latency="+StatUtils.mean(latencies));
		log.status(LogLevel.STATUS, "median latency="+StatUtils.percentile(latencies, 50));
		log.status(LogLevel.STATUS, "95-percentile latency="+StatUtils.percentile(latencies, 95));
		log.status(LogLevel.STATUS, "max latency="+StatUtils.max(latencies));
	}


	public void saveParseableQueriesToPlainFile(String filename) throws Exception {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(filename)));
		for (Q query : getAll_queries())
			bufferedWriter.write(query.getSql() + "\n");
		bufferedWriter.close();
	}
	
	public void saveParseableQueriesToTimestampQueriesFile(String filename) throws Exception {
		SimpleDateFormat dateFormat = new SimpleDateFormat(timeStampFormat);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(filename)));

		int writtenRecords = 0, failedRecs = 0;
		for (Q query : getAll_queries()) {
			try {
				Date mydate = query.getTimestamp();
				String dateStr = dateFormat.format(mydate);
				bufferedWriter.write(dateStr + fieldSeparator + query.getSql() + "\n");
				++writtenRecords;
			} catch (java.text.ParseException e1) {
				++failedRecs;
				e1.printStackTrace();
			}
		}
		bufferedWriter.close();
		System.out.println("wrote " + writtenRecords + " queries, and skipped "+failedRecs + " queries");
	}
	
	public List<Q> getAll_queries() {
		return all_queries;
	}
	
	public static void writeListOfQueryWindowsToSeparateFiles(File directory, List<QueryWindow> queryWindows) throws CloneNotSupportedException, Exception {
		List<List<String>> windows = new ArrayList<List<String>>();
		for (QueryWindow qWindow : queryWindows) {
			List<String> sqlList = new ArrayList<String>();
			for (Query q : qWindow.getQueries())
				sqlList.add(q.getSql());
			windows.add(sqlList);
		}
		writeListOfListOfQueriesToSeparateFiles(directory, windows);		
	}
	
	public static void writeListOfListOfQueriesToSeparateFiles(File directory, List<List<String>> windows) throws IOException {
		for (int w=0; w<windows.size(); ++w) {
			File wf = new File(directory, "w" + w + ".queries");
			WriteStringToFile(wf, StringUtils.Join(windows.get(w), "\n"));
		}
	}
	
	protected static void WriteStringToFile(File f, String s) throws IOException {
		PrintWriter pw = new PrintWriter(f);
		pw.println(s);
		pw.close();
	}

	public void writeLatencyHistogramToFile(String filename, int granularityInMilliSeconds) throws Exception {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(filename)));

		if (latencyMeter == null)
			throw new Exception("Your latencyMeter cannot be null");

		Map<Long, Integer> latencyInNewUnitCount = new HashMap<Long, Integer>();
		for (Q query : all_queries) {
			PerformanceRecord pr = new PerformanceRecord(query.getSql());
			latencyMeter.measureLatency(pr, new ArrayList<PhysicalStructure>(), "test", useExplainInsteadOfRunningQueries);
			PerformanceValueWithDesign pv = pr.getPerformanceValueWithDesign("test");
			long latency = pv.getMeanActualLatency();
			Long latencyInNewUnits;
			if (latency == Long.MAX_VALUE)
				latencyInNewUnits = 10*60000L / granularityInMilliSeconds;
			else
				latencyInNewUnits = latency / granularityInMilliSeconds;
			if (latencyInNewUnitCount.containsKey(latencyInNewUnits)) {
				int oldCnt = latencyInNewUnitCount.get(latencyInNewUnits);
				latencyInNewUnitCount.put(latencyInNewUnits, oldCnt + 1);
			} else
				latencyInNewUnitCount.put(latencyInNewUnits, 1);
		}
		
		List<Long> keysAsList = new ArrayList<Long>(latencyInNewUnitCount.keySet());
		Collections.sort(keysAsList);
		
		for (long latencyInNewUnit : keysAsList)
			bufferedWriter.append(latencyInNewUnit*granularityInMilliSeconds/(1000.0) + "\t" + latencyInNewUnitCount.get(latencyInNewUnit) + "\n");
		
		bufferedWriter.close();
	}
	
	public List<List<Q>> loadWindowsOfQueriesFromSeparateFiles(String directoryName, int maximumNumberOfWindows, int maximumNumberOfQueriesPerWindow) {
		List<List<String>> allWindows = loadWindowsOfQueryStringsFromSeparateFiles(directoryName, maximumNumberOfWindows, maximumNumberOfQueriesPerWindow);
		List<List<Q>> allWindowsQueries = new ArrayList<List<Q>>();
		
		for (int w=0; w<allWindows.size(); ++w) {
			List<Q> thisWindowQueries = new ArrayList<Q>();
			for (int q=0; q<allWindows.get(w).size(); ++q) {
				String sql = allWindows.get(w).get(q);
				Q query;
				try {
					query = qParser.parse(sql, schemaMap); 					
				} catch (Throwable t) {
					log.error("In file w" + w + ".queries, the " + q + "'th query could not parse; error: " + t.getMessage());
					continue;
				}
				thisWindowQueries.add(query);
			} // for q
			allWindowsQueries.add(thisWindowQueries);
		} // for w
				
		return allWindowsQueries;
	}

	public static List<List<String>> loadWindowsOfQueryStringsFromSeparateFiles(String directoryName, int maximumNumberOfWindows, int maximumNumberOfQueriesPerWindow) {
		assert (maximumNumberOfWindows==-1 || maximumNumberOfWindows>0);
		assert (maximumNumberOfQueriesPerWindow==-1 || maximumNumberOfQueriesPerWindow>0);
		if (!directoryName.endsWith("/"))
			directoryName += "/";
		
		List<List<String>> allWindows = new ArrayList<List<String>>();
		int w = 0;
		while (true) 
			try {
				if (allWindows.size()>=maximumNumberOfWindows && maximumNumberOfWindows!=-1)
					break;
				
				String filename = directoryName + "w" + w + ".queries";
				List<String> curWindow;
				curWindow = SqlLogFileManager.loadQueryStringsFromPlainFile(filename, maximumNumberOfQueriesPerWindow);
				allWindows.add(curWindow);
				log.status(LogLevel.VERBOSE, "loaded win_id=" + w + " from file "+ filename + " with "+  curWindow.size() + " queries\n");
				//List<Query_SWGO> curWindowQueries = new Query_SWGO.Generator().convertSqlToQuery(curWindow, schemaMap);
				//windowsQueriesSWGO.add(curWindowQueries);
				++w;
			} catch (IOException e) {
				if (!(e instanceof FileNotFoundException)) {
					log.error(e.getMessage());
					e.printStackTrace();
				}
				break;
			}
		
		assert(allWindows.size()==maximumNumberOfWindows || maximumNumberOfWindows==-1);
		return allWindows;
	}

	public static void main(String[] args) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(timeStampFormat);
		Date mydate;
		try {
			mydate = dateFormat.parse("2012-02-27 23:33:21.214393-08");
			System.out.println(dateFormat.format(mydate ));
		} catch (java.text.ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			String db_vendor = "vertica";
			//String db_vendor = "microsoft";
			String dbAlias = "dataset19";
			String directory = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/" + dbAlias + "/";

			String configFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf";
			List<DatabaseLoginConfiguration> allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(configFile, VerticaDatabaseLoginConfiguration.class.getSimpleName());
			//DatabaseLoginConfiguration designerDB = DatabaseLoginConfiguration.getEmptyDBs(allDatabaseConfigurations, dbAlias).get(0);
						
			String cacheFileName = directory + "dvals/experiment.cache";
			ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile(cacheFileName);
			
			DBDeployer dbDeployer = null; // new DBDeployer(LogLevel.STATUS, deployerDB, experimentCache, false);
			LatencyMeter latencyMeter;
			if (db_vendor.equals("vertica")) {
				DatabaseLoginConfiguration deployerDB = DatabaseLoginConfiguration.getFullDB(allDatabaseConfigurations, dbAlias);
				latencyMeter = new VerticaLatencyMeter(LogLevel.STATUS, true, deployerDB, experimentCache, dbDeployer, dbDeployer, 10*60);
			} else if (db_vendor.equals("microsoft")) {
				MicrosoftDatabaseLoginConfiguration microsoftDeployerDB = (MicrosoftDatabaseLoginConfiguration)DatabaseLoginConfiguration.getFullDB(allDatabaseConfigurations, dbAlias);
				latencyMeter = new MicrosoftLatencyMeter(LogLevel.STATUS, true, microsoftDeployerDB, experimentCache, dbDeployer, dbDeployer, 10*60);				
			} else {
				throw new Exception("Un-supported DB vendor: " + db_vendor);
			}
			DBDesigner dbDesigner = null;			
			
			
			//String sql = "select 1 from st_etl_2.ident_83 a left outer join st_etl_2.ident_83 b on a.ident_2669 = b.ident_2669 and b.ident_2251 = a.ident_2251";
			//String sql3= "select a.ident_2669, a.ident_2251, b.ident_2251, a.ident_451 from st_etl_2.ident_83 a left outer join st_etl_2.ident_83 b on a.ident_2669 = b.ident_2669 and b.ident_2251 = a.ident_2251";
			//String sql2 = "select * from v_monitor.query_requests where is_executing";
			//PerformanceRecord pr = new PerformanceRecord(sql);
			//PerformanceRecord pr2 = new PerformanceRecord(sql);
			//latencyMeter.measureLatency(pr, new ArrayList<VerticaProjectionStructure>(), "test");
			//latencyMeter.measureLatency(pr2, new ArrayList<VerticaProjectionStructure>(), "test");
			
			
			SchemaDescriptor schemaDesc = SchemaUtils.GetSchemaMap(dbAlias, allDatabaseConfigurations);
			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaDesc.getSchemas(), latencyMeter, false, dbDesigner); 
			//sqlLogFileManager.loadTimestampLatencyQueriesFromFile(directory + "out_dc_requests_issued-scrubbed");
			sqlLogFileManager.loadTimestampQueriesFromFile(directory + "parsed.timestamped");
			sqlLogFileManager.printLogFileStats();
			
			System.out.println("total # of queries: "+ sqlLogFileManager.getAll_queries().size()+" from "+
					sqlLogFileManager.getAll_queries().get(0).getTimestamp()+" to "+
					sqlLogFileManager.getAll_queries().get(sqlLogFileManager.getAll_queries().size()-1).getTimestamp());
			
			//sqlLogFileManager.saveParseableQueriesToPlainFile(directory + "parsed.plain");
			//sqlLogFileManager.saveParseableQueriesToTimestampQueriesFile(directory + "parsed.timestamped");
			sqlLogFileManager.saveParseableQueriesToTimestampQueriesFile(directory + "parsed-runnable-improvable.timestamped");
			
			sqlLogFileManager.writeLatencyHistogramToFile(directory + "latencyHistogram100ms.txt", 100);
			
			DistributionDistanceGenerator distanceGenerator = new DistributionDistancePair.Generator();
			UnpartitionedQueryLogAnalyzer<Query_SWGO> logAnalyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), distanceGenerator);
			logAnalyzer.sanityCheck();
			Clustering<Query_SWGO> clustering = new Clustering_QueryEquality();
			ClusteredWindow clusteredWindow = clustering.cluster(sqlLogFileManager.getAll_queries());
			
			System.out.println(clusteredWindow.toString());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
