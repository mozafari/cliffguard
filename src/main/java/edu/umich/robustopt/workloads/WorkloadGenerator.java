package edu.umich.robustopt.workloads;

import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryParser;

public abstract class WorkloadGenerator<D extends DistributionDistance, Q extends Query> {
	//public abstract void setDistributionDistance(D distance) throws Exception;		
	public abstract ClusteredWindow forecastNextWindow(ClusteredWindow originalWindow, D distance) throws Exception;
	public abstract QueryParser<Q> getQueryParser();
	public abstract Map<String, Schema> getSchemaMap();
	public abstract Clustering getClustering();
	
	public abstract Cluster createClusterWithNewFrequency(Cluster cluster, int newFreq) throws Exception;
	public abstract DistributionDistanceGenerator<D> getDistributionDistanceGenerator() throws Exception;
}
