package edu.umich.robustopt.clustering;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.workloads.WorkloadGenerator;

public abstract class Clustering<Q extends Query> {
	private Map<Cluster, Integer> clusterIdMapping = null;
	
	public abstract ClusteredWindow cluster(QueryWindow queryWindow) throws Exception;
	
	public ClusteredWindow cluster(List<Q> queryList) throws Exception {
		List<Query> queryList2 = new ArrayList<Query>();
		for (Q q : queryList)
			queryList2.add(q);
		
		return cluster(new QueryWindow(queryList2));
	}
	
	public ClusteredWindow assignClusterId(ClusteredWindow clustersWithoutId) throws Exception {
		if (clusterIdMapping == null)
			clusterIdMapping = new HashMap<Cluster, Integer>();
		
		List<Cluster> numberedClusters = new ArrayList<Cluster>();
		for (Cluster c : clustersWithoutId.getClusters()) {
			Cluster newCluster;
			if (c.getCluster_id()!=null)
				throw new Exception("The cluster has already been numbered: " + c.toString() + " with id: " + c.getCluster_id());
			if (clusterIdMapping.containsKey(c)) 
				newCluster = new Cluster(c.getQueries(), clusterIdMapping.get(c));
			else
				newCluster = new Cluster(c.getQueries(), clusterIdMapping.size()); 

			numberedClusters.add(newCluster);
			clusterIdMapping.put(c, newCluster.getCluster_id());
		}
		
		return new ClusteredWindow(numberedClusters);
	}
	
	public static void saveClustersToFile(ClusteredWindow clusters, String whereToSaveFiles) throws Exception {
		String sep = "\t";
		PrintStream clusterPdf = new PrintStream(new File(whereToSaveFiles, "clusterPdf"));
		PrintStream clusterDefs = new PrintStream(new File(whereToSaveFiles, "clusterDefs"));
		PrintStream clusterExamples = new PrintStream(new File(whereToSaveFiles, "clusterExamples"));
		
		List<Cluster> sortedClusters = new ArrayList<Cluster>(clusters.getClusters());
		Collections.sort(sortedClusters, new ClusterFrequencyComparator());
		
		int totalFreq = 0;
		for (Cluster cluster:sortedClusters) {
			totalFreq += cluster.getFrequency();
		}
		for (Cluster c:sortedClusters) {
			double p = c.getFrequency()/(double)totalFreq;
			clusterPdf.println(c.getCluster_id()+sep+p);
			clusterDefs.println(c.getCluster_id()+":  "+c.toString());
			clusterExamples.println(c.getCluster_id()+":> "+c.retrieveAQueryAtRandom().getSql());
		}

		clusterPdf.close();
		clusterDefs.close();
		clusterExamples.close();		
	}
	
	public ClusteredWindow convertSqlListIntoClusteredWindow(List<String> sqlQueries, QueryParser<Q> queryParser, Map<String, Schema> schemaMap) throws Exception {
		List<Q> queries = queryParser.convertSqlListToQuery(sqlQueries, schemaMap);
		ClusteredWindow clusteredWindow = cluster(queries);
		return clusteredWindow;
	}

	public ClusteredWindow convertSqlListIntoClusteredWindow(List<String> sqlQueries, WorkloadGenerator workloadGenerator) throws Exception {
		return convertSqlListIntoClusteredWindow(sqlQueries, workloadGenerator.getQueryParser(), workloadGenerator.getSchemaMap());
	}

	
}
