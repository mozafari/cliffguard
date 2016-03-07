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

public abstract class LogFileBasedEuclideanDistanceWorkloadGenerator extends Query_SWGO_WorkloadGenerator<EuclideanDistance> {
	protected Double penaltyForGoingFromZeroToNonZero = 1d;
	protected List<Query> exampleSqlQueries = null;
	protected Map<Cluster, Cluster> clusterMap = null;
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> exampleSqlQueries, Double penaltyForGoingFromZeroToNonZero) throws Exception {
		super(schema, constManager);
		if (exampleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (exampleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> exampleQueries_SWGO = getQueryParser().convertSqlListToQuery(exampleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(exampleQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		System.out.println("Created a LogFileBasedEuclideanDistanceWorkloadGenerator with " + uniqueQueries.size() + " unique queries.");
		this.exampleSqlQueries = new ArrayList<Query>(uniqueQueries);
		this.penaltyForGoingFromZeroToNonZero = penaltyForGoingFromZeroToNonZero;
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> exampleSqlQueries) throws Exception {
		this(schema, constManager, exampleSqlQueries, 1d);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, String databaseLoginFile, String DBVendor, 
			List<String> exampleSqlQueries) throws Exception{
		super(dbName, databaseLoginFile, DBVendor);
		if (exampleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (exampleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> exampleSqlQueries_SWGO = getQueryParser().convertSqlListToQuery(exampleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(exampleSqlQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		this.exampleSqlQueries = new ArrayList<Query>(uniqueQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema, String dbName, 
			double samplingRate, File f, List<String> exampleSqlQueries) throws Exception{
		super(schema, dbName, samplingRate, f);
		if (exampleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (exampleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> exampleQueries_SWGO = getQueryParser().convertSqlListToQuery(exampleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(exampleQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		this.exampleSqlQueries = new ArrayList<Query>(uniqueQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema, List<String> exampleSqlQueries) throws Exception {
		this(schema, null, exampleSqlQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, String DBVendor, List<String> exampleSqlQueries) throws Exception{
		this(SchemaUtils.GetSchemaMapFromDefaultSources(dbName, DatabaseLoginConfiguration.getDatabaseSpecificLoginName(DBVendor)).getSchemas(), null, exampleSqlQueries);
	}

	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, List<DatabaseLoginConfiguration> dbLogins, List<String> exampleSqlQueries) throws Exception{
		this(SchemaUtils.GetSchemaMap(dbName, dbLogins).getSchemas(), null, exampleSqlQueries);
	}
		
	@Override
	public Cluster createClusterWithNewFrequency(Cluster cluster, int newFreq) throws Exception {
		if (clusterMap == null) {
			Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
			ClusteredWindow clusterdWindow = clusteringQueryEquality.cluster(exampleSqlQueries);
			this.clusterMap = new HashMap<Cluster, Cluster>();
			for(Cluster c : clusterdWindow.getClusters() ){
				clusterMap.put(c, c);
			}
		}
		if(newFreq <= cluster.getFrequency()){
			return new Cluster(cluster.getQueries().subList(0, newFreq));
		} else {
			List<Query> retClusterList = new ArrayList(cluster.getQueries()); 
			int nToAdd = newFreq - cluster.getFrequency();
			Cluster logCluster = clusterMap.get(cluster);
			// If clusterMap contains cluster, we get queries from clusterMap. Otherwise, we randomly duplicate existing queries. 
			for(int i = 0; i < nToAdd; ++i) 
				retClusterList.add(clusterMap.containsKey(cluster) ? logCluster.retrieveAQueryAtRandom() : cluster.retrieveAQueryAtRandom());
			return new Cluster(retClusterList);
		}
	}
}
