package edu.umich.robustopt.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusteredWindow implements Cloneable {
	private Set<Cluster> clusters;
	
	public ClusteredWindow(Set<Cluster> clusters) throws CloneNotSupportedException {
		this.clusters = new HashSet<Cluster>();
		for (Cluster c : clusters)
			this.clusters.add(c.clone());
	}
	
	@Override
	public ClusteredWindow clone() throws CloneNotSupportedException {
		Set<Cluster> newClusters = new HashSet<Cluster>();
		for (Cluster c : clusters)
			newClusters.add(c.clone());

		ClusteredWindow newClusteredWindow = new ClusteredWindow(newClusters);
		
		return newClusteredWindow;
	}
	
	// this constructor carefully goes the elements in the list and merged those that are identical!
	public ClusteredWindow(List<Cluster> clusterList) throws Exception {
		Map<Cluster, Cluster> cMap = new HashMap<Cluster, Cluster>();
		for (Cluster c : clusterList)
			if (!cMap.containsKey(c))
				cMap.put(c, c);
			else {
				Cluster otherC = cMap.get(c);
				otherC = Cluster.createMergedCluster(otherC, c);
				cMap.put(otherC, otherC);				
			}
		
		this.clusters = new HashSet<Cluster>();
		for (Cluster c : cMap.keySet())
			clusters.add(c.clone());
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ClusteredWindow:\n");
		if (clusters == null) 
			sb.append("Error: null clusters");
		else 
			for (Cluster c : clusters) {
				try {
					sb.append("\t" + c.getFrequency() + ":" + c.retrieveAQueryAtPosition(0).getSql() + "\n");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		return sb.toString();
	}
	
	public String showFrequencyDistribution() {
		StringBuilder sb = new StringBuilder();
		sb.append("ClusteredWindow: ");
		if (clusters == null) 
			sb.append("Error: null clusters");
		else 
			for (Cluster c : clusters) {
				try {
					sb.append(" " + c.getFrequency());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		sb.append("\n");
		
		return sb.toString();
	}
	
	public int numberOfClusters() {
		return clusters.size();
	}
	
	public int totalNumberOfQueries() {
		int totalFreq = 0;
		for (Cluster c:clusters) { 
			totalFreq += c.getFrequency();
		}
		return totalFreq;			
	}
	
	public Set<Cluster> getClusters() throws CloneNotSupportedException {
		return Collections.unmodifiableSet(clusters);
	}
	
	public double getFraction(Cluster cluster) {
		if (!clusters.contains(cluster)) {
			System.err.println("Something is fishy: " + clusters + " looking for " + cluster);
			return 0.0; 
		} else {
			double totalFreq = 0.0;
			int soughtFreq = 0;
			for (Cluster c:clusters) { 
				totalFreq += c.getFrequency();
				if (c.equals(cluster))
					soughtFreq = c.getFrequency();
			}
			return soughtFreq / totalFreq;			
		}		
	}
	
	public List<String> getAllSql() throws Exception {
		List<String> allSql = new ArrayList<String>();
		for (Cluster cluster : clusters) 
			allSql.addAll(cluster.getAllSql());
		return allSql;
	}

	public List<Query> getAllQueries() throws Exception {
		List<Query> allQueries = new ArrayList<Query>();
		for (Cluster cluster : clusters) 
			allQueries.addAll(cluster.getQueries());
		return allQueries;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((clusters == null) ? 0 : clusters.hashCode());
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
		ClusteredWindow other = (ClusteredWindow) obj;
		if (clusters == null) {
			if (other.clusters != null)
				return false;
		} else if (!clusters.equals(other.clusters))
			return false;
		return true;
	}
	
	public int countQueryOccurrence(String query) {
		int count = 0;
		for (Cluster c : clusters)
			count += c.countQueryOccurrence(query);
		return count;
	}
	
}
