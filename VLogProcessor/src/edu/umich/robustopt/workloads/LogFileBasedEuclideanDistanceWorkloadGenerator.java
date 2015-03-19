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
	protected List<Query> allPossibleLogQueries = null;
	protected Map<Cluster, Cluster> clusterMap = null;
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> allPossibleSqlQueries, Double penaltyForGoingFromZeroToNonZero) throws Exception {
		super(schema, constManager);
		if (allPossibleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (allPossibleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> allPossibleLogQueries_SWGO = getQueryParser().convertSqlListToQuery(allPossibleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(allPossibleLogQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		System.out.println("Created a LogFileBasedEuclideanDistanceWorkloadGenerator with " + uniqueQueries.size() + " unique queries.");
		this.allPossibleLogQueries = new ArrayList<Query>(uniqueQueries);
		this.penaltyForGoingFromZeroToNonZero = penaltyForGoingFromZeroToNonZero;
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema,
			ConstantValueManager constManager, List<String> allPossibleSqlQueries) throws Exception {
		this(schema, constManager, allPossibleSqlQueries, 1d);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, String databaseLoginFile, String DBVendor, 
			List<String> allPossibleSqlQueries) throws Exception{
		super(dbName, databaseLoginFile, DBVendor);
		if (allPossibleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (allPossibleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> allPossibleLogQueries_SWGO = getQueryParser().convertSqlListToQuery(allPossibleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(allPossibleLogQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		this.allPossibleLogQueries = new ArrayList<Query>(uniqueQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema, String dbName, 
			double samplingRate, File f, List<String> allPossibleSqlQueries) throws Exception{
		super(schema, dbName, samplingRate, f);
		if (allPossibleSqlQueries == null) {
			throw new Exception("The list of queries should not be null");
		} else if (allPossibleSqlQueries.size() == 0) {
			throw new Exception("The list of queries should not be empty");
		}
		List<Query_SWGO> allPossibleLogQueries_SWGO = getQueryParser().convertSqlListToQuery(allPossibleSqlQueries, schema);
		List<Query> logQueries = Query.convertToListOfQuery(allPossibleLogQueries_SWGO);
		Set<Query> uniqueQueries = new HashSet<Query>();
		for (Query q : logQueries) {
			if (uniqueQueries.contains(q)) {
				continue;
			} else {
				uniqueQueries.add(q);
			}
		}
		this.allPossibleLogQueries = new ArrayList<Query>(uniqueQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(Map<String, Schema> schema, List<String> allPossibleSqlQueries) throws Exception {
		this(schema, null, allPossibleSqlQueries);
	}
	
	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, String DBVendor, List<String> allPossibleSqlQueries) throws Exception{
		this(SchemaUtils.GetSchemaMapFromDefaultSources(dbName, DBVendor).getSchemas(), null, allPossibleSqlQueries);
	}

	public LogFileBasedEuclideanDistanceWorkloadGenerator(String dbName, List<DatabaseLoginConfiguration> dbLogins, List<String> allPossibleSqlQueries) throws Exception{
		this(SchemaUtils.GetSchemaMap(dbName, dbLogins).getSchemas(), null, allPossibleSqlQueries);
	}
		
	@Override
	public Cluster createClusterWithNewFrequency(Cluster cluster, int newFreq) throws Exception {
		if (clusterMap == null) {
			Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
			ClusteredWindow clusterdWindow = clusteringQueryEquality.cluster(allPossibleLogQueries);
			this.clusterMap = new HashMap<Cluster, Cluster>();
			for(Cluster c : clusterdWindow.getClusters() ){
				clusterMap.put(c, c);
			}
		}
		if(clusterMap.containsKey(cluster)){
			Cluster logCluster = clusterMap.get(cluster);
			if(newFreq <= logCluster.getFrequency()){
				return new Cluster(logCluster.getQueries().subList(0, newFreq));
			} else {
				List<Query> retClusterList = new ArrayList(logCluster.getQueries()); 
				int nToAdd = newFreq - logCluster.getFrequency();
				for(int i = 0; i < nToAdd; ++i){
					retClusterList.add(logCluster.retrieveAQueryAtRandom());
				}
				return new Cluster(retClusterList);
			}
		} else {
			return null;
		}
	}
	
}
