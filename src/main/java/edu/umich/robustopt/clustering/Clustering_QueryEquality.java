package edu.umich.robustopt.clustering;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.robustopt.clustering.Query;


public class Clustering_QueryEquality extends Clustering {

	public ClusteredWindow cluster(QueryWindow queryWindow) throws Exception {
		Map<Query, Cluster> allClusters = new HashMap<Query, Cluster>();
			
		for (Query q : queryWindow.getQueries()) {// getQueries is returning a cloned copy of the queries!
			Cluster c;
			if (allClusters.containsKey(q))
				c = Cluster.createMergedCluster(allClusters.get(q), new Cluster(q));
			else
				c = new Cluster(q);
			
			allClusters.put(q, c);
		}
		
		List<Cluster> clusterList = new ArrayList<Cluster>();
		clusterList.addAll(allClusters.values());
		ClusteredWindow clusteredWindow = new ClusteredWindow(clusterList);
		
		return clusteredWindow;
	}

}
