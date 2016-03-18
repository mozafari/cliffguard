package edu.umich.robustopt.workloads;

import java.util.List;

import edu.umich.robustopt.clustering.Query;

public interface DistributionDistanceGenerator<T extends DistributionDistance> {
	//public T createDistance(ClusteredWindow leftClusteredWindow, ClusteredWindow rightClusteredWindow) throws Exception;
	public T distance(List<Query> leftWindow, List<Query> rightWindow) throws Exception;
}
