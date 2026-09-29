package edu.umich.robustopt.clustering;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaConnection;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion;
import edu.umich.robustopt.workloads.EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong;

/*
 * This is the class file that we gave Vivek to run on his DB dumps so we could decide which dumps were more meaningful to us...
 */
public class TestWhichDumpIsBetter {
	public enum FileFormats {
		PlainFile, TimestampedFile, InvalidFormat
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 7) {
			System.out.println("Usage: java -jar thisFile.jar format DBhost DBport DBname DBuser DBpasswd queryFile");
			System.out.println("The format is one of the following: ");
			System.out.println("\tplain\t\t If the file only contains a single SQL per line");
			System.out.println("\ttimestamped\t\t If the file has 'timestamp|sql query' per line, where | is the field separator");
			
			return;
		}
		String formatStr = args[0];
		String DBhost = args[1];
		Integer DBport = Integer.parseInt(args[2]);
		String DBname = args[3];
		String DBuser = args[4];
		String DBpassword = args[5];
		String queryFileName = args[6];
		
		FileFormats formatIdx = FileFormats.InvalidFormat;
		if (formatStr.equals("plain")) {
			formatIdx = FileFormats.PlainFile;
		} else if (formatStr.equals("timestamped")) {
			formatIdx = FileFormats.TimestampedFile;
		} else {
			System.err.println("Unrecognized file format: " + formatStr);
			return;
		}
		
		boolean isEmpty = true;
		String dbAlias = /*"dataset" +*/ DBname;
		String whereToStoreSchemaCache = "/dev/null"; // + dbAlias+".schema.ser";
		
		DatabaseLoginConfiguration dbLogin = new VerticaDatabaseLoginConfiguration(isEmpty, dbAlias, DBhost, DBport, DBname, DBuser, DBpassword);
		System.out.println("Login object created!");
		
		try {
			Connection connection = VerticaConnection.createConnection(dbLogin);
			System.out.println("Connection created!");
			SchemaDescriptor schemaDesc = SchemaUtils.GetSchemaMap(dbAlias, whereToStoreSchemaCache, dbLogin);
			
			//SchemaDescriptor schemaDesc = SchemaUtils.GetSchemaMap(dbAlias);
			System.out.println("SchemaDescriptor loaded from the database: " + schemaDesc.getSummary());
			
			//
			LatencyMeter latencyMeter = null;
			Boolean useExplainInsteadOfRunningQueries = null;
			DBDesigner dbDesigner = null;
			SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', ";barzan", new Query_SWGO.QParser(), schemaDesc.getSchemas(), latencyMeter, useExplainInsteadOfRunningQueries, dbDesigner); 
			
			switch (formatIdx) {
			case PlainFile:
				List<String> queriesStr = SqlLogFileManager.loadQueryStringsFromPlainFile(queryFileName, -1);
				
				break;
				
			case TimestampedFile:
				System.out.println("**********************************************************************");
				System.out.println("Dataset: " + dbAlias);
				
				List<Query_SWGO> queries = sqlLogFileManager.loadTimestampQueriesFromFile(queryFileName);
				sqlLogFileManager.printLogFileStats();
				if (sqlLogFileManager.getAll_queries().isEmpty()) {
					System.err.println("Seems like none of the queries have been successfully parsed!");
					System.err.println("Terminating the task for the current dataset!");
					return;
				}
				// not empty! 
				System.out.println("total # of queries: "+ sqlLogFileManager.getAll_queries().size()+" from "+
						sqlLogFileManager.getAll_queries().get(0).getTimestamp()+" to "+
						sqlLogFileManager.getAll_queries().get(sqlLogFileManager.getAll_queries().size()-1).getTimestamp());
				
				sqlLogFileManager.saveParseableQueriesToTimestampQueriesFile(queryFileName + ".parsed.timestamped"); 
							
				int windowSizeInDays = 28;
				
				DistributionDistanceGenerator EuclideanDistanceGenerator = new EuclideanDistanceWithSimpleUnion.Generator(schemaDesc.getSchemas(), EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
				UnpartitionedQueryLogAnalyzer<Query_SWGO> logAnalyzer = new UnpartitionedQueryLogAnalyzer<Query_SWGO>(new Query_SWGO.QParser(), sqlLogFileManager.getAll_queries(), EuclideanDistanceGenerator);
				
				logAnalyzer.sanityCheck();
				
				List<QueryWindow> windows = logAnalyzer.splitIntoTimeEqualWindows(windowSizeInDays);
				
				Clustering<Query_SWGO> clustering = new Clustering_QueryEquality();
				
				System.out.print("When splitting into windowSize="+windowSizeInDays + " days, this dataset had: ");
				int totalQueries = 0;
				for (int w=0; w<windows.size(); ++w) { 
					QueryWindow thisWindow = windows.get(w);
					ClusteredWindow clusteredWindow = clustering.cluster(thisWindow);
					System.out.print("w="+w +": " + thisWindow.getQueries().size() + " queries (" + clusteredWindow.getClusters().size() + " uniques), ");
					totalQueries += thisWindow.getQueries().size();
				}
			
				System.out.println();
				System.out.println("Spanning " + windows.size() + " months with a total of " + totalQueries + " queries.");
				System.out.println("**********************************************************************");

				// second distance!
				//DistributionDistanceGenerator PairDistanceGenerator = new DistributionDistancePair.Generator();
				
				break;
				
			default:
				throw new Exception("Invalid file format!");
			}
									

			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
