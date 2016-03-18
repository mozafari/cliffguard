package edu.umich.robustopt.clustering;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.stat.StatUtils;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;

import edu.umich.robustopt.util.BLog;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.BLog.LogLevel;

public class SqlLogManager {
	static BLog log = new BLog(LogLevel.DEBUG);

	public static char separator = '|';
	private List<Query> all_queries = null;
	private Map<String, Schema> schemaMap = null;
	private double minLatencyInSecs = 5.0;
	private int number_of_all_queries = 0;
	private int number_of_ignored_by_dbd = 0;
	private int number_of_ignored_by_parser = 0;
	private int number_of_ignored_for_latency = 0;
	private int number_of_corrupted_queries = 0;
	private int number_of_empty_queries = 0;

	
	public SqlLogManager(String input_query_log, String schemaAlias) throws Exception {
		schemaMap = SchemaUtils.GetSchemaMap(schemaAlias).getSchemas();
		all_queries = load_queries(input_query_log);
	}

	private void printLogFileStats(){
		System.out.println("number_of_all_queries: "+number_of_all_queries);
		System.out.println("number_of_ignored_by_dbd: "+number_of_ignored_by_dbd);
		System.out.println("number_of_ignored_by_parser: "+number_of_ignored_by_parser);
		System.out.println("number_of_ignored_for_latency: "+number_of_ignored_for_latency + " for a threshold of "+ minLatencyInSecs +" secs");
		System.out.println("number_of_corrupted_queries: "+number_of_corrupted_queries);
		System.out.println("number_of_empty_queries: "+ number_of_empty_queries);

	}

	private List<Query> load_queries(String input_query_log) throws Exception {
		 BufferedReader in = new BufferedReader(new FileReader(input_query_log));
		 
		 String line;
		 int lineNumber = 0;
		 Parser p;
		 List<Query> loaded_queries = new ArrayList<Query>();		 
		 SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 
		 while ((line=in.readLine())!=null) {
			 ++lineNumber;
			 ++number_of_all_queries;
			 int pos1 = line.indexOf(SqlLogManager.separator);			 
			 int pos2 = line.indexOf(SqlLogManager.separator, pos1+1);
			 if (pos1 == -1 || pos2 ==-1) {
				 log.status(LogLevel.STATUS, input_query_log + " lineNumber " + lineNumber + " no separator!");
				 ++number_of_corrupted_queries;
				 continue;
			 }
			 String timestampStr = line.substring(0, pos1);
			 String latencyStr = line.substring(pos1+1, pos2);
			 String sql = line.substring(pos2+1);
			  
 			 Date timestamp;
			 try {
				 int pos3 = timestampStr.indexOf('.');
				 timestampStr = timestampStr.substring(0, pos3);
				 if (pos3 == -1) {
					 log.status(LogLevel.STATUS, "Bad timestamp: "+timestampStr);
					 throw new ParseException( "Bad timestamp: "+timestampStr);
				 }				
			 	 timestamp = (Date) formatter.parse(timestampStr);
			 } catch (ParseException e) {
				 ++number_of_corrupted_queries;
				 e.printStackTrace();
				 continue;
			 } 
			 double latency = Double.parseDouble(latencyStr);
			 if (latency<minLatencyInSecs) {
				 ++number_of_ignored_for_latency;
				 continue;
			 }
				 			 
			 try {
				 Query_v2 query = new Query_v2(sql, schemaMap);
				 if (query.isEmpty()) {
					 ++ number_of_empty_queries;
				 	 continue;
				 }
				 query.addDetails(lineNumber, timestamp, latency, sql);
				 loaded_queries.add(query);
				 log.status(LogLevel.VERSBOE, "<line: " + lineNumber + "> " + query.toString());
				 log.status(LogLevel.VERSBOE, "summarized as: " + query.toString());			 
			 } catch (Throwable t) {
				//log_error(input_query_log, lineNumber, t.getMessage());
				++ number_of_ignored_by_parser;
				continue;
			 }
		 }
		 in.close();
		 return loaded_queries;
	}
	
	private static void print_latency_stats(Collection<Query> queries) throws Exception {
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

	public static void main(String[] args) {
		try {
			SqlLogManager sqlLogManager = new SqlLogManager("data/slower1_considered", "dataset1");
			System.out.println("total # of queries:"+sqlLogManager.getAll_queries().size()+" from "+
					sqlLogManager.getAll_queries().get(0).getTimestamp()+" to "+
					sqlLogManager.getAll_queries().get(sqlLogManager.getAll_queries().size()-1).getTimestamp());
			
			DistributionAnalyzer distributionAnalyzer = new DistributionAnalyzer(sqlLogManager.getAll_queries());
			distributionAnalyzer.sanityCheck();
			distributionAnalyzer.analyzeConceptShift("data/mat/concept_shift");
			distributionAnalyzer.analyzeWindowSize("data/mat/winsize");
			Map<Query,Cluster> all_clusters = ClusteringQuery_v2.cluster(sqlLogManager.getAll_queries());
			ClusteringQuery_v2.save_clusters(new ClusteredWindow(all_clusters.values()), "data/mat/clusters");
			print_latency_stats(sqlLogManager.getAll_queries());
			sqlLogManager.printLogFileStats();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<Query> getAll_queries() {
		return all_queries;
	}

	
	
}
