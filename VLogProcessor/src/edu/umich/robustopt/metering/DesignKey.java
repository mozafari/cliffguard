package edu.umich.robustopt.metering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.clustering.WeightedQuery;


public class DesignKey implements Serializable {

	private static final long serialVersionUID = -1477077651447575630L;
	private Set<WeightedQuery> weightedQueries;
	protected String algorithmName = null;
	private String algorithmParams = null;
	
	public static DesignKey createDesignKeyByQueries(List<String> queries) {
		DesignKey key = createDesignKeyByQueriesAlgorithm(queries, null, null);
		return key;
	}
	
	public static DesignKey createDesignKeyByQueriesAlgorithm(List<String> queries, String algorithmName, String algorithmParams) {
		DesignKey key = new DesignKey();
		key.weightedQueries = WeightedQuery.consolidateQueries(queries);
		key.algorithmName = algorithmName;
		key.algorithmParams = algorithmParams;
		return key;
	}
	
	public static DesignKey createDesignKeyByWeightedQueries(List<WeightedQuery> weightedQueries) {
		DesignKey key = createDesignKeyByWeightedQueriesAlgorithms(weightedQueries, null, null);
		return key;
	}
	
	public static DesignKey createDesignKeyByWeightedQueriesAlgorithms(List<WeightedQuery> weightedQueries, String algorithmName, String algorithmParams) {
		DesignKey key = new DesignKey();
		key.weightedQueries = WeightedQuery.consolidateWeightedQueies(weightedQueries);
		key.algorithmName = algorithmName;
		key.algorithmParams = algorithmParams;
		return key;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((algorithmName == null) ? 0 : algorithmName.hashCode());
		result = prime * result
				+ ((algorithmParams == null) ? 0 : algorithmParams.hashCode());
		result = prime * result
				+ ((weightedQueries == null) ? 0 : weightedQueries.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DesignKey other = (DesignKey) obj;
		if (algorithmName == null) {
			if (other.algorithmName != null)
				return false;
		} else if (!algorithmName.equals(other.algorithmName))
			return false;
		if (algorithmParams == null) {
			if (other.algorithmParams != null)
				return false;
		} else if (!algorithmParams.equals(other.algorithmParams))
			return false;
		if (weightedQueries == null) {
			if (other.weightedQueries != null)
				return false;
		} else if (!weightedQueries.equals(other.weightedQueries))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return (algorithmName==null? "null" : algorithmName) + " : " + (algorithmParams==null? "null" : algorithmParams) + " : " + weightedQueries;
	}

	public String getAlgorithmName() {
		return algorithmName;
	}

	public String getAlgorithmParams() {
		return algorithmParams;
	}

	public Set<WeightedQuery> getWeightedQueries() {
		return weightedQueries;
	}
	
	
}
