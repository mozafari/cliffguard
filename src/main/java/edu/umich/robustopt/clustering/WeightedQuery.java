package edu.umich.robustopt.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.robustopt.algorithms.NoDesigner;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.metering.PerformanceValue;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class WeightedQuery implements Serializable {
	private static final long serialVersionUID = 5996686805410008255L;
	public String query;
	public Double weight;
	public WeightedQuery(String query, Double weight) {
		super();
		this.query = query;
		this.weight = weight;
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((query == null) ? 0 : query.hashCode());
		result = prime * result + ((weight == null) ? 0 : weight.hashCode());
		return result;
	}

	public static List<WeightedQuery> mergeQueryAndWeightsIntoOneList(List<String> queries, List<Double> weights) {
		if (queries == null || weights == null || queries.size()!=weights.size())
			throw new IllegalArgumentException("should be not null and same length!");
		List<WeightedQuery> result = new ArrayList<WeightedQuery>();
		for (int i=0; i<queries.size(); ++i)
			result.add(new WeightedQuery(queries.get(i), weights.get(i)));
		
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
		WeightedQuery other = (WeightedQuery) obj;
		if (query == null) {
			if (other.query != null)
				return false;
		} else if (!query.equals(other.query))
			return false;
		if (weight == null) {
			if (other.weight != null)
				return false;
		} else if (!weight.equals(other.weight))
			return false;
		return true;
	}



	@Override
	public String toString() {
		return "<weight=" + weight + ", query=" + query + ">";
	}
	
	/*
	 * This function can creates a weighted list of queries where the weights are the latency of each query.
	 * These latencies come from one of these two sources:
	 * 1) these latencies can be those measured using a design (which will be extracted using the name of the design algorithm, called latencyGeneratingAlgorithm)
	 * or 2) by measuing those latencies directly (using the latencyMeter object) in which no projections will be used (i.e., using an empty design).
	 */
	public static List<WeightedQuery> populateWeightsUsingLatencies(List<PerformanceRecord> performanceRecords, String latencyGeneratingAlgorithm, LatencyMeter latencyMeter, boolean useExplainInsteadOfRunningQueries) throws Exception {
		List<Double> latencies = new ArrayList<Double>();
		List<String> queries = new ArrayList<String>();
		
		for (int q=0; q<performanceRecords.size(); ++q) {
			queries.add(performanceRecords.get(q).getQuery());
			Double latency;
			if (latencyGeneratingAlgorithm != null) {
				PerformanceValue perfValue = performanceRecords.get(q).getPerformanceValueWithDesign(latencyGeneratingAlgorithm);
				if (perfValue == null) 
					throw new Exception("You provided latencyGeneratingAlgorithm="+latencyGeneratingAlgorithm+ " but it was not present in the performanceRecord: " 
							+ performanceRecords.get(q));
				latency = (double) perfValue.getMeanLatency();
			} else {
				PerformanceRecord perfRecord = new PerformanceRecord(queries.get(q));
				latencyMeter.measureLatency(perfRecord, new ArrayList<PhysicalStructure>(), NoDesigner.class.getCanonicalName(), useExplainInsteadOfRunningQueries);
				latency = (double) (perfRecord.getPerformanceValueWithDesign(NoDesigner.class.getCanonicalName()).getMeanLatency());
			}
			latencies.add(latency);
		}
		List<WeightedQuery> weightedQueries = WeightedQuery.mergeQueryAndWeightsIntoOneList(queries, latencies);
		return weightedQueries;
	}
	
	public static Set<WeightedQuery> consolidateWeightedQueies(List<WeightedQuery> weightedQueries) {
		Map<String, Double> queryToWeight = new HashMap<String, Double>();
		
		for (WeightedQuery wq : weightedQueries) {
			String key = wq.query.toLowerCase();
			if (queryToWeight.containsKey(key)) {
				Double oldWeight = queryToWeight.get(key);
				queryToWeight.put(key, oldWeight + wq.weight);
			} else {
				queryToWeight.put(key, wq.weight);
			}
		}
		
		Set<WeightedQuery> consolidatedQueries = new HashSet<WeightedQuery>();
		
		for (String q : queryToWeight.keySet()) {
			Double w = queryToWeight.get(q);
			consolidatedQueries.add(new WeightedQuery(q, w));
		}
		
		System.out.println("reduced " + weightedQueries.size() + " queries to "+ consolidatedQueries.size() + " queries");
		return consolidatedQueries;
	}

	public static Set<WeightedQuery> consolidateQueries(List<String> queries) {
		List<WeightedQuery> weightedQueries = new ArrayList<WeightedQuery>();
		
		for (String q : queries)
			weightedQueries.add(new WeightedQuery(q, 1.0));
		
		return consolidateWeightedQueies(weightedQueries);
	}

}