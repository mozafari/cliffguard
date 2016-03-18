package edu.umich.robustopt.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.umich.robustopt.common.Randomness;

public class Cluster implements Cloneable {
	private List<Query> queries = new ArrayList<Query>();
	private Integer cluster_id = null;
	
	public Cluster(List<Query> queries, Integer cluster_id) throws CloneNotSupportedException {
		for (int i=1; i<queries.size(); ++i)
			if (!queries.get(i).equals(queries.get(0))) {
				System.err.println("BUG: adding different queries to the same cluster: 0'th query=" + queries.get(0) + ", and " + i + "'th query=" + queries.get(i));
			}
		
		for (int i=0; i<queries.size(); ++i)
			this.queries.add(queries.get(i).clone());

		this.cluster_id = cluster_id;
	}
	
	public Cluster(List<Query> queries) throws CloneNotSupportedException {
		this(queries, null);
	}

	public Cluster(Query singleQuery) throws CloneNotSupportedException {
		this(Arrays.asList(singleQuery), null);
	}
	
	@Override
	public Cluster clone() throws CloneNotSupportedException {
		Integer new_cluster_id = cluster_id;
		List<Query> newQueries = new ArrayList<Query>();
		for (int i=0; i<queries.size(); ++i)
			newQueries.add(queries.get(i).clone());
		
		Cluster newCluster = new Cluster(newQueries, new_cluster_id);
		
		return newCluster;
	}
	
	public Query retrieveAQueryAtRandom() throws Exception {
		if (queries.size()==0) {
			throw new Exception("Cannot get random element, size = 0 ");	
		}		
		int index = Randomness.randGen.nextInt(queries.size());
		return retrieveAQueryAtPosition(index);
	}

	
	public Query retrieveAQueryAtPosition(int index) throws Exception {
		if (index<0 || index>=queries.size()) {
			throw new Exception("Invalid index: "+index+" for size: "+queries.size());			
		}		
		return queries.get(index).clone();
	}

	public List<String> getAllSql() throws Exception {
		List<String> allSql = new ArrayList<String>();
		for (Query query : queries) {
			allSql.add(query.getSql());
		}
		return allSql;
	}
	
	public int getFrequency() {
		return queries.size();
	}

	public Integer getCluster_id() {
		return cluster_id;
	}
	
	public List<Query> getQueries() {
		return Collections.unmodifiableList(queries);
	}

	public static Cluster createMergedCluster(Cluster first, Cluster second) throws Exception {
		if (!first.equals(second))
			throw new Exception("The queries in the two clusters are not equal: " + first + " and " + second);
		
		Integer cluster_id;
		if (first.cluster_id==null || (second.cluster_id!=null && second.cluster_id<first.cluster_id))
			cluster_id = second.cluster_id;
		else 
			cluster_id = first.cluster_id;
		
		List<Query> queries = new ArrayList<Query>();
		for (int i=0; i<first.getFrequency(); ++i)
			queries.add(first.retrieveAQueryAtPosition(i));
		for (int i=0; i<second.getFrequency(); ++i)
			queries.add(second.retrieveAQueryAtPosition(i));
		
		return new Cluster(queries, cluster_id);		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queries == null) ? 0 : queries.get(0).hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Cluster))
			return false;
		Cluster other = (Cluster) obj;
		if (queries == null) {
			if (other.queries != null)
				return false;
		} else if (!queries.get(0).equals(other.queries.get(0)))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		String str;
		try {
			str = queries.get(0).getSql();
		} catch (Exception e) {
			str = "threw an exceptions!";
		}
		
		return "{Cluster: " + getFrequency() + " queries, e.g.: " + str + "}";
	}
	
	public int countQueryOccurrence(String query) {
		int count = 0;
		for (int i=0; i<queries.size(); ++i) {
			try {
				if (query.toLowerCase().equals(queries.get(i).getSql().toLowerCase()))
					++count;
			} catch (Exception e) {
			}
		}
		return count;
	}
	
}
