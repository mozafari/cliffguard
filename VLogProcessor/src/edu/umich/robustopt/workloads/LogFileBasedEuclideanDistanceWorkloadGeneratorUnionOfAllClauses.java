package edu.umich.robustopt.workloads;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

public abstract class LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses extends
	LogFileBasedEuclideanDistanceWorkloadGenerator {
	protected Set<UnionOption> whichClauses = null;
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> exampleSqlQueries, Set<UnionOption> whichClauses, Double penaltyForGoingFromZeroToNonZero) throws Exception {
		super(schema, constManager, exampleSqlQueries, penaltyForGoingFromZeroToNonZero);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.whichClauses = whichClauses;
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception {
		this(schema, constManager, exampleSqlQueries, whichClauses, 1d);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(String dbName, String databaseLoginFile, String DBVendor, 
			List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception{
		super(dbName, databaseLoginFile, DBVendor, exampleSqlQueries);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.whichClauses = whichClauses;
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(Map<String, Schema> schema, String dbName, 
			double samplingRate, File f, List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception{
		super(schema, dbName, samplingRate, f, exampleSqlQueries);
		if (whichClauses.isEmpty()) {
			throw new Exception("Should at least has a clause in option");
		}
		this.whichClauses = whichClauses;
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(Map<String, Schema> schema, List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception {
		this(schema, null, exampleSqlQueries, whichClauses);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(String dbName, String DBVendor, List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception{
		this(SchemaUtils.GetSchemaMapFromDefaultSources(dbName, DatabaseLoginConfiguration.getDatabaseSpecificLoginName(DBVendor)).getSchemas(), null, exampleSqlQueries, whichClauses);
	}

	public LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses(String dbName, List<DatabaseLoginConfiguration> dbLogins, List<String> exampleSqlQueries, Set<UnionOption> whichClauses) throws Exception{
		this(SchemaUtils.GetSchemaMap(dbName, dbLogins).getSchemas(), null, exampleSqlQueries, whichClauses);
	}
}
