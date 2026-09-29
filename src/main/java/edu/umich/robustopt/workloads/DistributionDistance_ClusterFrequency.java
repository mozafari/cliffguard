package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.stat.StatUtils;
import org.apache.commons.math.stat.StatUtils.*;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.util.MyMathUtils;

/*
 * This class represents the distance between two windows of queries 
 */
public class DistributionDistance_ClusterFrequency extends DistributionDistance {
	private Double numberOfClustersLeft = Double.NaN;
	private Double numberOfClustersRight = Double.NaN;
	private Double numberOfRepresentedQueriesLeft = Double.NaN;
	private Double numberOfRepresentedQueriesRight = Double.NaN;
	private Double avgAbsoluteProbabilityChangeForNonZerosInLeftToRight = Double.NaN;
	private Double avgRelativeProbabilityChangeForNonZerosInLeftToRight = Double.NaN;
	private Double avgAbsoluteProbabilityChangeForNonZerosInBothWindows = Double.NaN;
	private Double avgRelativeProbabilityChangeForNonZerosInBothWindows = Double.NaN;
	private Double avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = Double.NaN;
	private Double avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight = Double.NaN;
			
	private Double medianAbsoluteProbabilityChangeForNonZerosInLeftToRight = Double.NaN;
	private Double medianRelativeProbabilityChangeForNonZerosInLeftToRight = Double.NaN;
	private Double medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = Double.NaN;
	
	private Double numberOfCommonClusters = Double.NaN;
	private Double numberOfExpiredClusters = Double.NaN;
	private Double numberOfNewClusters = Double.NaN;
	
	private Double ratioOfCommonClustersInLeft = Double.NaN;
	private Double ratioOfExpiredClustersOfLeft = Double.NaN;
	private Double ratioOfCommonClustersInRight = Double.NaN;
	private Double ratioOfNewClustersInRight = Double.NaN;
	
	private Double numberOfCommonQueriesInLeft = Double.NaN;
	private Double numberOfCommonQueriesInRight = Double.NaN;
	private Double numberOfExpiredQueriesFromLeft = Double.NaN;
	private Double numberOfNewQueriesInRight = Double.NaN;
	
	private Double ratioOfCommonQueriesInLeft = Double.NaN;
	private Double ratioOfExpiredQueriesOfLeft = Double.NaN;
	private Double ratioOfCommonQueriesInRight = Double.NaN;
	private Double ratioOfNewQueriesInRight = Double.NaN;

	private Double medianLatencyOfCommonQueriesInLeft = Double.NaN;
	private Double medianLatencyOfExpiredQueriesOfLeft = Double.NaN;
	private Double medianLatencyOfCommonQueriesInRight = Double.NaN;
	private Double medianLatencyOfNewQueriesInRight = Double.NaN;
	
	private Double ratioOfLatencyOfCommonQueriesInLeft = Double.NaN;
	private Double ratioOfLatencyOfExpiredQueriesOfLeft = Double.NaN;
	private Double ratioOfLatencyOfCommonQueriesInRight = Double.NaN;
	private Double ratioOfLatencyOfNewQueriesInRight = Double.NaN;
	
	private ClusteredWindow leftClusteredWindow = null;
	private ClusteredWindow rightClusteredWindow = null;
	
	private Integer howManyPairsAreRepresentedByThisObject = 0;

	private DistributionDistance_ClusterFrequency() {
		
	}
	
	private DistributionDistance_ClusterFrequency(ClusteredWindow leftClusteredWindow, ClusteredWindow rightClusteredWindow) throws Exception {
		this.leftClusteredWindow = leftClusteredWindow.clone();
		this.rightClusteredWindow = rightClusteredWindow.clone();
		this.howManyPairsAreRepresentedByThisObject = 1;
	}
	
	/*
	private boolean areBothClusters(Collection<Query> leftWindow, Collection<Query> rightWindow) throws Exception {
		boolean undetermined = true;
		boolean areClusters = true;

		List<Query> both = new ArrayList<Query>();
		both.addAll(leftWindow);
		both.addAll(rightWindow);
		for (Query q:both) { 
			if (q.isCluster()) {
				if (undetermined) {
					undetermined = false;
					areClusters = true;
				} else if (!areClusters)
					throw new Exception("Queries and clusters are mixed together!");
			} else {
				if (undetermined) {
					undetermined = false;
					areClusters = false;
				} else if (areClusters)
					throw new Exception("Queries and clusters are mixed together!");
			}	
		}		
		return areClusters;
	}*/
	
	/*
	 * processes this.leftWindow and this.rightWindow as two sets of clusters and computes all the measures
	 */

	public static class Generator implements DistributionDistanceGenerator<DistributionDistance_ClusterFrequency> {
		@Override
		public DistributionDistance_ClusterFrequency distance(List<Query> leftWindow, List<Query> rightWindow) throws Exception {
			Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
			ClusteredWindow leftClusters = clusteringQueryEquality.cluster(leftWindow);
			ClusteredWindow rightClusters = clusteringQueryEquality.cluster(rightWindow);
			
			DistributionDistance_ClusterFrequency dist = distance(leftClusters,rightClusters);
			
			return dist;
		}
		
		private DistributionDistance_ClusterFrequency distance(ClusteredWindow leftWindow, ClusteredWindow rightWindow) throws Exception {
			DistributionDistance_ClusterFrequency dist = new DistributionDistance_ClusterFrequency(leftWindow, rightWindow);
			// We now start making all the measurements!
			dist.numberOfClustersLeft = (double) leftWindow.numberOfClusters();
			dist.numberOfClustersRight = (double) rightWindow.numberOfClusters();
			
			dist.numberOfRepresentedQueriesLeft = (double) 0;		
			for (Cluster cluster:leftWindow.getClusters())  
				dist.numberOfRepresentedQueriesLeft += cluster.getFrequency();
			
			dist.numberOfRepresentedQueriesRight = (double) 0;
			for (Cluster cluster:rightWindow.getClusters())
				dist.numberOfRepresentedQueriesRight += cluster.getFrequency();
					
			Set<Cluster> commonFromLeft = new HashSet<Cluster>();
			Set<Cluster> uniqueInLeft = new HashSet<Cluster>();
			for (Cluster cluster:leftWindow.getClusters())
				if (rightWindow.getClusters().contains(cluster))
					commonFromLeft.add(cluster);
				else
					uniqueInLeft.add(cluster);
	
			Set<Cluster> commonFromRight = new HashSet<Cluster>();		
			Set<Cluster> uniqueInRight = new HashSet<Cluster>();
			for (Cluster cluster:rightWindow.getClusters())
				if (leftWindow.getClusters().contains(cluster))
					commonFromRight.add(cluster);
				else
					uniqueInRight.add(cluster);
			
			//Now use these common and unique sets to calculate more interesting metrics 		
			
			double[] absoluteProbabilityChangeForNonZerosInLeftToRight = new double[uniqueInLeft.size()+commonFromLeft.size()]; 				
			double[] relativeProbabilityChangeForNonZerosInLeftToRight = new double[uniqueInLeft.size()+commonFromLeft.size()]; 		
			double[] absoluteProbabilityChangeForNonZerosInBothWindows = new double[commonFromLeft.size()]; 				
			double[] relativeProbabilityChangeForNonZerosInBothWindows = new double[commonFromLeft.size()]; 		
			double[] absoluteProbabilityBetweenNonZerosInEitherWindow = new double[uniqueInLeft.size()+commonFromLeft.size()+uniqueInRight.size()]; 
			double[] absoluteProbabilityChangeForZeroInLeftToNonZeroInRight = new double[uniqueInRight.size()];
			
			Map<Cluster, Cluster> commonInRightMap = new HashMap<Cluster, Cluster>();
			for (Cluster cluster:commonFromRight)
				commonInRightMap.put(cluster, cluster);
			
			int next=0;
			for (Cluster cluster:commonFromLeft) {
				double oldProb = cluster.getFrequency() / (double) dist.numberOfRepresentedQueriesLeft;
				double newProb = commonInRightMap.get(cluster).getFrequency() / (double) dist.numberOfRepresentedQueriesRight;
				absoluteProbabilityChangeForNonZerosInLeftToRight[next] = Math.abs(oldProb-newProb);																	 
				relativeProbabilityChangeForNonZerosInLeftToRight[next] = Math.abs(oldProb-newProb) / oldProb;
				absoluteProbabilityChangeForNonZerosInBothWindows[next] = Math.abs(oldProb-newProb);																	 
				relativeProbabilityChangeForNonZerosInBothWindows[next] = Math.abs(oldProb-newProb) / oldProb;
				++next;
			}
			for (Cluster cluster:uniqueInLeft) {
				absoluteProbabilityChangeForNonZerosInLeftToRight[next] = cluster.getFrequency() / (double) dist.numberOfRepresentedQueriesLeft;
				relativeProbabilityChangeForNonZerosInLeftToRight[next] = 1.0;
				++next;
			}
			for (int i=0; i<next; ++i)
				absoluteProbabilityBetweenNonZerosInEitherWindow[i] = absoluteProbabilityChangeForNonZerosInLeftToRight[i];
			for (Cluster cluster:uniqueInRight) {
				absoluteProbabilityBetweenNonZerosInEitherWindow[next] = cluster.getFrequency() / (double) dist.numberOfRepresentedQueriesRight;
				++next;
			}
			next = 0;
			for (Cluster cluster:uniqueInRight) {
				absoluteProbabilityChangeForZeroInLeftToNonZeroInRight[next] = cluster.getFrequency() / (double) dist.numberOfRepresentedQueriesRight;
				++next;
			}
			
			dist.avgAbsoluteProbabilityChangeForNonZerosInLeftToRight = StatUtils.mean(absoluteProbabilityChangeForNonZerosInLeftToRight);
			dist.avgRelativeProbabilityChangeForNonZerosInLeftToRight = StatUtils.mean(relativeProbabilityChangeForNonZerosInLeftToRight);
			dist.avgAbsoluteProbabilityChangeForNonZerosInBothWindows = StatUtils.mean(absoluteProbabilityChangeForNonZerosInBothWindows);
			dist.avgRelativeProbabilityChangeForNonZerosInBothWindows = StatUtils.mean(relativeProbabilityChangeForNonZerosInBothWindows);
			
			if (dist.avgAbsoluteProbabilityChangeForNonZerosInBothWindows < 0 || dist.avgAbsoluteProbabilityChangeForNonZerosInBothWindows > 1) {
				System.err.println("We found a bug: ");
				for (int i=0; i<relativeProbabilityChangeForNonZerosInBothWindows.length; ++i)
					System.err.print(relativeProbabilityChangeForNonZerosInBothWindows[i] + " ");
				
			}
			
			dist.avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = StatUtils.mean(absoluteProbabilityBetweenNonZerosInEitherWindow);
			dist.avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight = StatUtils.mean(absoluteProbabilityChangeForZeroInLeftToNonZeroInRight);
	
			
			dist.medianAbsoluteProbabilityChangeForNonZerosInLeftToRight = StatUtils.percentile(absoluteProbabilityChangeForNonZerosInLeftToRight, 50.0);
			dist.medianRelativeProbabilityChangeForNonZerosInLeftToRight = StatUtils.percentile(relativeProbabilityChangeForNonZerosInLeftToRight, 50.0);
			dist.medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = StatUtils.percentile(absoluteProbabilityBetweenNonZerosInEitherWindow, 50.0);
			
			dist.numberOfCommonClusters =  (double) commonFromLeft.size();
			dist.numberOfExpiredClusters = (double) uniqueInLeft.size();
			dist.numberOfNewClusters = (double) uniqueInRight.size();
			
			dist.ratioOfCommonClustersInLeft = commonFromLeft.size() / (double)leftWindow.numberOfClusters();
			dist.ratioOfExpiredClustersOfLeft = uniqueInLeft.size() / (double)leftWindow.numberOfClusters();
			dist.ratioOfCommonClustersInRight = commonFromRight.size() / (double)rightWindow.numberOfClusters();
			dist.ratioOfNewClustersInRight = uniqueInRight.size() / (double)rightWindow.numberOfClusters();
	
			dist.numberOfCommonQueriesInLeft = (double) 0;
			for (Cluster cluster:commonFromLeft)
				dist.numberOfCommonQueriesInLeft += cluster.getFrequency();
	
			dist.numberOfCommonQueriesInRight = (double) 0;
			for (Cluster cluster:commonFromRight)
				dist.numberOfCommonQueriesInRight += cluster.getFrequency();
					
			dist.numberOfExpiredQueriesFromLeft = (double) 0;
			for (Cluster cluster:uniqueInLeft)
				dist.numberOfExpiredQueriesFromLeft += cluster.getFrequency();
	
			dist.numberOfNewQueriesInRight = (double) 0;
			for (Cluster cluster:uniqueInRight)
				dist.numberOfNewQueriesInRight += cluster.getFrequency();
					
			dist.ratioOfCommonQueriesInLeft = dist.numberOfCommonQueriesInLeft / (double)(dist.numberOfCommonQueriesInLeft+dist.numberOfExpiredQueriesFromLeft);
			dist.ratioOfExpiredQueriesOfLeft = dist.numberOfExpiredQueriesFromLeft / (double)(dist.numberOfCommonQueriesInLeft+dist.numberOfExpiredQueriesFromLeft);
			dist.ratioOfCommonQueriesInRight = dist.numberOfCommonQueriesInRight / (double)(dist.numberOfCommonQueriesInRight+dist.numberOfNewQueriesInRight);
			dist.ratioOfNewQueriesInRight = dist.numberOfNewQueriesInRight / (double)(dist.numberOfCommonQueriesInRight+dist.numberOfNewQueriesInRight);
	
			Double[] latencyOfCommonQueriesInLeft = new Double[(int)(double)dist.numberOfCommonQueriesInLeft];
			Double[] latencyOfExpiredQueriesOfLeft = new Double[(int)(double)dist.numberOfExpiredQueriesFromLeft];
			Double[] latencyOfCommonQueriesInRight = new Double[(int)(double)dist.numberOfCommonQueriesInRight];
			Double[] latencyOfNewQueriesOfRight = new Double[(int)(double)dist.numberOfNewQueriesInRight];
			
			next = 0;
			for (Cluster cluster:commonFromLeft) {
				int c = cluster.getFrequency();
				for (int i=0; i<c; ++i) {
					latencyOfCommonQueriesInLeft[next] = cluster.retrieveAQueryAtPosition(i).getOriginalLatency();
					++next;
				}
			}
			next = 0;
			for (Cluster cluster:uniqueInLeft) {
				int c = cluster.getFrequency();
				for (int i=0; i<c; ++i) {
					latencyOfExpiredQueriesOfLeft[next] = cluster.retrieveAQueryAtPosition(i).getOriginalLatency();
					++next;
				}
			}
			next = 0;
			for (Cluster cluster:commonFromRight) {
				int c = cluster.getFrequency();
				for (int i=0; i<c; ++i) {
					latencyOfCommonQueriesInRight[next] = cluster.retrieveAQueryAtPosition(i).getOriginalLatency();
					++next;
				}
			}
			next = 0;
			for (Cluster cluster:uniqueInRight) {
				int c = cluster.getFrequency();
				for (int i=0; i<c; ++i) {
					latencyOfNewQueriesOfRight[next] = cluster.retrieveAQueryAtPosition(i).getOriginalLatency();
					++next;
				}
			}
			
			dist.medianLatencyOfCommonQueriesInLeft = MyMathUtils.percentile(latencyOfCommonQueriesInLeft, 50.0);
			dist.medianLatencyOfExpiredQueriesOfLeft = MyMathUtils.percentile(latencyOfExpiredQueriesOfLeft, 50.0);
			dist.medianLatencyOfCommonQueriesInRight = MyMathUtils.percentile(latencyOfCommonQueriesInRight, 50.0);
			dist.medianLatencyOfNewQueriesInRight = MyMathUtils.percentile(latencyOfNewQueriesOfRight, 50.0);
				
			Double all1 = (latencyOfCommonQueriesInLeft.length>0 ? MyMathUtils.sum(latencyOfCommonQueriesInLeft) : 0.0) 
											 + (latencyOfExpiredQueriesOfLeft.length>0 ? MyMathUtils.sum(latencyOfExpiredQueriesOfLeft) : 0.0);
			dist.ratioOfLatencyOfCommonQueriesInLeft = MyMathUtils.sum(latencyOfCommonQueriesInLeft) / all1; 
			dist.ratioOfLatencyOfExpiredQueriesOfLeft = MyMathUtils.sum(latencyOfExpiredQueriesOfLeft) / all1;
			Double all2 = (latencyOfCommonQueriesInRight.length>0 ? MyMathUtils.sum(latencyOfCommonQueriesInRight) : 0.0)
											 + (latencyOfNewQueriesOfRight.length>0 ? MyMathUtils.sum(latencyOfNewQueriesOfRight) : 0.0);
			dist.ratioOfLatencyOfCommonQueriesInRight = MyMathUtils.sum(latencyOfCommonQueriesInRight) / all2;
			dist.ratioOfLatencyOfNewQueriesInRight = MyMathUtils.sum(latencyOfNewQueriesOfRight) / all2;
			
			return dist;	
		}

	}

	/*
	@Override
	public String toString() {
		return showDifferences();
	}
	*/
	
	
	public String showSummary() {
		StringBuffer sb = new StringBuffer();
		sb.append("numberOfClustersLeft= "+ numberOfClustersLeft + "\n");
		sb.append("numberOfClustersRight= "+ numberOfClustersRight + "\n");
		sb.append("numberOfRepresentedQueriesLeft= "+ numberOfRepresentedQueriesLeft + "\n");
		sb.append("numberOfRepresentedQueriesRight= "+ numberOfRepresentedQueriesRight + "\n");
		sb.append("avgAbsoluteProbabilityChangeForNonZerosInLeftToRight= "+ avgAbsoluteProbabilityChangeForNonZerosInLeftToRight + "\n");
		sb.append("avgRelativeProbabilityChangeForNonZerosInLeftToRight= "+ avgRelativeProbabilityChangeForNonZerosInLeftToRight + "\n");
		sb.append("avgAbsoluteProbabilityChangeForNonZerosInBothWindows= "+ avgAbsoluteProbabilityChangeForNonZerosInBothWindows + "\n");
		sb.append("avgRelativeProbabilityChangeForNonZerosInBothWindows= "+ avgRelativeProbabilityChangeForNonZerosInBothWindows + "\n");
		sb.append("avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow= "+ avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow + "\n");
		sb.append("avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight= "+ avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight + "\n");
		                                
		sb.append("medianAbsoluteProbabilityChangeForNonZerosInLeftToRight= "+ medianAbsoluteProbabilityChangeForNonZerosInLeftToRight + "\n");
		sb.append("medianRelativeProbabilityChangeForNonZerosInLeftToRight= "+ medianRelativeProbabilityChangeForNonZerosInLeftToRight + "\n");
		sb.append("medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow= "+ medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow + "\n");
		                
		sb.append("numberOfCommonClusters= "+ numberOfCommonClusters + "\n");
		sb.append("numberOfExpiredClusters= "+ numberOfExpiredClusters + "\n");
		sb.append("numberOfNewClusters= "+ numberOfNewClusters + "\n");
		                
		sb.append("ratioOfCommonClustersInLeft= "+ ratioOfCommonClustersInLeft + "\n");
		sb.append("ratioOfExpiredClustersOfLeft= "+ ratioOfExpiredClustersOfLeft + "\n");
		sb.append("ratioOfCommonClustersInRight= "+ ratioOfCommonClustersInRight + "\n");
		sb.append("ratioOfNewClustersInRight= "+ ratioOfNewClustersInRight + "\n");
		                
		sb.append("numberOfCommonQueriesInLeft= "+ numberOfCommonQueriesInLeft + "\n");
		sb.append("numberOfCommonQueriesInRight= "+ numberOfCommonQueriesInRight + "\n");
		sb.append("numberOfExpiredQueriesFromLeft= "+ numberOfExpiredQueriesFromLeft + "\n");
		sb.append("numberOfNewQueriesInRight= "+ numberOfNewQueriesInRight + "\n");
		                
		sb.append("ratioOfCommonQueriesInLeft= "+ ratioOfCommonQueriesInLeft + "\n");
		sb.append("ratioOfExpiredQueriesOfLeft= "+ ratioOfExpiredQueriesOfLeft + "\n");
		sb.append("ratioOfCommonQueriesInRight= "+ ratioOfCommonQueriesInRight + "\n");
		sb.append("ratioOfNewQueriesInRight= "+ ratioOfNewQueriesInRight + "\n");

		sb.append("medianLatencyOfCommonQueriesInLeft= "+ medianLatencyOfCommonQueriesInLeft + "\n");
		sb.append("medianLatencyOfExpiredQueriesOfLeft= "+ medianLatencyOfExpiredQueriesOfLeft + "\n");
		sb.append("medianLatencyOfCommonQueriesInRight= "+ medianLatencyOfCommonQueriesInRight + "\n");
		sb.append("medianLatencyOfNewQueriesInRight= "+ medianLatencyOfNewQueriesInRight + "\n");

		sb.append("ratioOfLatencyOfCommonQueriesInLeft= "+ ratioOfLatencyOfCommonQueriesInLeft + "\n");
		sb.append("ratioOfLatencyOfExpiredQueriesOfLeft= "+ ratioOfLatencyOfExpiredQueriesOfLeft + "\n");
		sb.append("ratioOfLatencyOfCommonQueriesInRight= "+ ratioOfLatencyOfCommonQueriesInRight + "\n");
		sb.append("ratioOfLatencyOfNewQueriesInRight= "+ ratioOfLatencyOfNewQueriesInRight + "\n");
		
		return sb.toString();
	}

	public String showConciseSummary() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("numberOfClustersLeft=" + numberOfClustersLeft);
		sb.append("\n");
		sb.append("numberOfRepresentedQueriesLeft=" + numberOfRepresentedQueriesLeft);
		sb.append("\n");
		sb.append("avgAbsoluteProbabilityChangeForNonZerosInLeftToRight=" + avgAbsoluteProbabilityChangeForNonZerosInLeftToRight);
		sb.append("\n");
		sb.append("avgRelativeProbabilityChangeForNonZerosInLeftToRight=" + avgRelativeProbabilityChangeForNonZerosInLeftToRight);
		sb.append("\n");
		sb.append("avgAbsoluteProbabilityChangeForNonZerosInBothWindows=" + avgAbsoluteProbabilityChangeForNonZerosInBothWindows);
		sb.append("\n");
		sb.append("avgRelativeProbabilityChangeForNonZerosInBothWindows=" + avgRelativeProbabilityChangeForNonZerosInBothWindows);
		sb.append("\n");
		sb.append("avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight=" + avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight);
		sb.append("\n");
		sb.append("numberOfCommonClusters=" + numberOfCommonClusters);
		sb.append("\n");
		sb.append("numberOfExpiredClusters=" + numberOfExpiredClusters);
		sb.append("\n");
		sb.append("ratioOfCommonQueriesInRight=" + ratioOfCommonQueriesInRight);
		sb.append("\n");
		sb.append("d-values=(" + Math.round(avgRelativeProbabilityChangeForNonZerosInBothWindows*100)/100.0 + "," + 
								Math.round(numberOfExpiredClusters/numberOfClustersLeft*100)/100.0 + "," + 
								2*Math.round(avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight*100)/100.0 + ")");
		sb.append("\n");
		return sb.toString();
	}
	
	@Override
	public DistributionDistance computeAverage(DistributionDistance firstD, DistributionDistance secondD) throws Exception {
		if (!(firstD instanceof DistributionDistance_ClusterFrequency) || !(secondD instanceof DistributionDistance_ClusterFrequency))
			throw new Exception("Cannot average incompatible types of distances: DistributionDistance_ClusterFrequency and "+ secondD.getClass().getCanonicalName());
		DistributionDistance_ClusterFrequency second = (DistributionDistance_ClusterFrequency) secondD;
		DistributionDistance_ClusterFrequency first = (DistributionDistance_ClusterFrequency) firstD;
		DistributionDistance_ClusterFrequency avg = new DistributionDistance_ClusterFrequency();
		
		int n1 = first.howManyPairsAreRepresentedByThisObject;
		int n2 = second.howManyPairsAreRepresentedByThisObject;
		double sum = n1+n2;
		double r1 = n1/sum, r2 = n2/sum;
		avg.numberOfClustersLeft = MyMathUtils.weightedAvg(n1, first.numberOfClustersLeft, n2, second.numberOfClustersLeft);
		avg.numberOfClustersRight = MyMathUtils.weightedAvg(n1, first.numberOfClustersRight, n2, second.numberOfClustersRight);
		avg.numberOfRepresentedQueriesLeft = MyMathUtils.weightedAvg(n1, first.numberOfRepresentedQueriesLeft, n2, second.numberOfRepresentedQueriesLeft);
		avg.numberOfRepresentedQueriesRight = MyMathUtils.weightedAvg(n1, first.numberOfRepresentedQueriesRight, n2, second.numberOfRepresentedQueriesRight);
		
		avg.avgAbsoluteProbabilityChangeForNonZerosInLeftToRight = MyMathUtils.weightedAvg(n1, first.avgAbsoluteProbabilityChangeForNonZerosInLeftToRight, n2, second.avgAbsoluteProbabilityChangeForNonZerosInLeftToRight);
		avg.avgRelativeProbabilityChangeForNonZerosInLeftToRight = MyMathUtils.weightedAvg(n1, first.avgRelativeProbabilityChangeForNonZerosInLeftToRight, n2, second.avgRelativeProbabilityChangeForNonZerosInLeftToRight);
		avg.avgAbsoluteProbabilityChangeForNonZerosInBothWindows = MyMathUtils.weightedAvg(n1, first.avgAbsoluteProbabilityChangeForNonZerosInBothWindows, n2, second.avgAbsoluteProbabilityChangeForNonZerosInBothWindows);
		avg.avgRelativeProbabilityChangeForNonZerosInBothWindows = MyMathUtils.weightedAvg(n1, first.avgRelativeProbabilityChangeForNonZerosInBothWindows, n2, second.avgRelativeProbabilityChangeForNonZerosInBothWindows);
		avg.avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = MyMathUtils.weightedAvg(n1, first.avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow, n2, second.avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow);
		avg.avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight = MyMathUtils.weightedAvg(n1, first.avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight, n2, second.avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight);
		
		avg.medianAbsoluteProbabilityChangeForNonZerosInLeftToRight = MyMathUtils.weightedAvg(n1, first.medianAbsoluteProbabilityChangeForNonZerosInLeftToRight, n2, second.medianAbsoluteProbabilityChangeForNonZerosInLeftToRight);
		avg.medianRelativeProbabilityChangeForNonZerosInLeftToRight = MyMathUtils.weightedAvg(n1, first.medianRelativeProbabilityChangeForNonZerosInLeftToRight, n2, second.medianRelativeProbabilityChangeForNonZerosInLeftToRight);
		avg.medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow = MyMathUtils.weightedAvg(n1, first.medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow, n2, second.medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow);
		
		avg.numberOfCommonClusters = MyMathUtils.weightedAvg(n1, first.numberOfCommonClusters, n2, second.numberOfCommonClusters);
		avg.numberOfExpiredClusters = MyMathUtils.weightedAvg(n1, first.numberOfExpiredClusters, n2, second.numberOfExpiredClusters);
		avg.numberOfNewClusters = MyMathUtils.weightedAvg(n1, first.numberOfNewClusters, n2, second.numberOfNewClusters);
		
		avg.ratioOfCommonClustersInLeft = MyMathUtils.weightedAvg(n1, first.ratioOfCommonClustersInLeft, n2, second.ratioOfCommonClustersInLeft);
		avg.ratioOfExpiredClustersOfLeft = MyMathUtils.weightedAvg(n1, first.ratioOfExpiredClustersOfLeft, n2, second.ratioOfExpiredClustersOfLeft);
		avg.ratioOfCommonClustersInRight = MyMathUtils.weightedAvg(n1, first.ratioOfCommonClustersInRight, n2, second.ratioOfCommonClustersInRight);
		avg.ratioOfNewClustersInRight = MyMathUtils.weightedAvg(n1, first.ratioOfNewClustersInRight, n2, second.ratioOfNewClustersInRight);
		
		avg.numberOfCommonQueriesInLeft = MyMathUtils.weightedAvg(n1, first.numberOfCommonQueriesInLeft, n2, second.numberOfCommonQueriesInLeft);
		avg.numberOfCommonQueriesInRight = MyMathUtils.weightedAvg(n1, first.numberOfCommonQueriesInRight, n2, second.numberOfCommonQueriesInRight);
		avg.numberOfExpiredQueriesFromLeft = MyMathUtils.weightedAvg(n1, first.numberOfExpiredQueriesFromLeft, n2, second.numberOfExpiredQueriesFromLeft);
		avg.numberOfNewQueriesInRight = MyMathUtils.weightedAvg(n1, first.numberOfNewQueriesInRight, n2, second.numberOfNewQueriesInRight);
		
		avg.ratioOfCommonQueriesInLeft = MyMathUtils.weightedAvg(n1, first.ratioOfCommonQueriesInLeft, n2, second.ratioOfCommonQueriesInLeft);
		avg.ratioOfExpiredQueriesOfLeft = MyMathUtils.weightedAvg(n1, first.ratioOfExpiredQueriesOfLeft, n2, second.ratioOfExpiredQueriesOfLeft);
		avg.ratioOfCommonQueriesInRight = MyMathUtils.weightedAvg(n1, first.ratioOfCommonQueriesInRight, n2, second.ratioOfCommonQueriesInRight);
		avg.ratioOfNewQueriesInRight = MyMathUtils.weightedAvg(n1, first.ratioOfNewQueriesInRight, n2, second.ratioOfNewQueriesInRight);
		
		avg.medianLatencyOfCommonQueriesInLeft = MyMathUtils.weightedAvg(n1, first.medianLatencyOfCommonQueriesInLeft, n2, second.medianLatencyOfCommonQueriesInLeft);
		avg.medianLatencyOfExpiredQueriesOfLeft = MyMathUtils.weightedAvg(n1, first.medianLatencyOfExpiredQueriesOfLeft, n2, second.medianLatencyOfExpiredQueriesOfLeft);
		avg.medianLatencyOfCommonQueriesInRight = MyMathUtils.weightedAvg(n1, first.medianLatencyOfCommonQueriesInRight, n2, second.medianLatencyOfCommonQueriesInRight);
		avg.medianLatencyOfNewQueriesInRight = MyMathUtils.weightedAvg(n1, first.medianLatencyOfNewQueriesInRight, n2, second.medianLatencyOfNewQueriesInRight);
		
		avg.ratioOfLatencyOfCommonQueriesInLeft = MyMathUtils.weightedAvg(n1, first.ratioOfLatencyOfCommonQueriesInLeft, n2, second.ratioOfLatencyOfCommonQueriesInLeft);
		avg.ratioOfLatencyOfExpiredQueriesOfLeft = MyMathUtils.weightedAvg(n1, first.ratioOfLatencyOfExpiredQueriesOfLeft, n2, second.ratioOfLatencyOfExpiredQueriesOfLeft);
		avg.ratioOfLatencyOfCommonQueriesInRight = MyMathUtils.weightedAvg(n1, first.ratioOfLatencyOfCommonQueriesInRight, n2, second.ratioOfLatencyOfCommonQueriesInRight);
		avg.ratioOfLatencyOfNewQueriesInRight = MyMathUtils.weightedAvg(n1, first.ratioOfLatencyOfNewQueriesInRight, n2, second.ratioOfLatencyOfNewQueriesInRight);
		
		avg.leftClusteredWindow = null;
		avg.rightClusteredWindow = null;		
		avg.howManyPairsAreRepresentedByThisObject = n1 + n2;
		
		return avg;
	}
	
	
	public Integer getNumberOfClustersLeft() {
		return (Integer)(int)(double)numberOfClustersLeft;
	}

	public Integer getNumberOfClustersRight() {
		return (Integer)(int)(double)numberOfClustersRight;
	}

	public Integer getNumberOfRepresentedQueriesLeft() {
		return (Integer)(int)(double)numberOfRepresentedQueriesLeft;
	}

	public Integer getNumberOfRepresentedQueriesRight() {
		return (Integer)(int)(double)numberOfRepresentedQueriesRight;
	}

	public Double getAvgAbsoluteProbabilityChangeForNonZerosInLeftToRight() {
		return avgAbsoluteProbabilityChangeForNonZerosInLeftToRight;
	}

	public Double getAvgRelativeProbabilityChangeForNonZerosInLeftToRight() {
		return avgRelativeProbabilityChangeForNonZerosInLeftToRight;
	}

	public Double getAvgAbsoluteProbabilityChangeForNonZerosInBothWindows() {
		return avgAbsoluteProbabilityChangeForNonZerosInBothWindows;
	}

	public Double getAvgRelativeProbabilityChangeForNonZerosInBothWindows() {
		return avgRelativeProbabilityChangeForNonZerosInBothWindows;
	}

	public Double getAvgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow() {
		return avgAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow;
	}

	public Double getAvgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight() {
		return avgAbsoluteProbabilityChangeForZeroInLeftToNonZeroInRight;
	}

	public Double getMedianAbsoluteProbabilityChangeForNonZerosInLeftToRight() {
		return medianAbsoluteProbabilityChangeForNonZerosInLeftToRight;
	}

	public Double getMedianRelativeProbabilityChangeForNonZerosInLeftToRight() {
		return medianRelativeProbabilityChangeForNonZerosInLeftToRight;
	}

	public Double getMedianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow() {
		return medianAbsoluteProbabilityChangeBetweenNonZerosInEitherWindow;
	}

	public Integer getNumberOfCommonClusters() {
		return (numberOfCommonClusters==null? null : (int)(double)numberOfCommonClusters);
	}

	public Integer getNumberOfExpiredClusters() {
		return (numberOfExpiredClusters==null? null : (int)(double)numberOfExpiredClusters);
	}

	public Integer getNumberOfNewClusters() {
		return (numberOfNewClusters==null? null : (int)(double)numberOfNewClusters);
	}

	public Double getRatioOfCommonClustersInLeft() {
		return ratioOfCommonClustersInLeft;
	}

	public Double getRatioOfExpiredClustersOfLeft() {
		return ratioOfExpiredClustersOfLeft;
	}

	public Double getRatioOfCommonClustersInRight() {
		return ratioOfCommonClustersInRight;
	}

	public Double getRatioOfNewClustersInRight() {
		return ratioOfNewClustersInRight;
	}

	public Integer getNumberOfCommonQueriesInLeft() {
		return (numberOfCommonQueriesInLeft==null? null : (int)(double)numberOfCommonQueriesInLeft);
	}

	public Integer getNumberOfCommonQueriesInRight() {
		return (numberOfCommonQueriesInRight==null? null : (int)(double)numberOfCommonQueriesInRight);
	}

	public Integer getNumberOfExpiredQueriesFromLeft() {
		return (numberOfExpiredQueriesFromLeft==null? null : (int)(double)numberOfExpiredQueriesFromLeft);
	}

	public Integer getNumberOfNewQueriesInRight() {
		return (numberOfNewQueriesInRight==null? null : (int)(double)numberOfNewQueriesInRight);
	}

	public Double getRatioOfCommonQueriesInLeft() {
		return ratioOfCommonQueriesInLeft;
	}

	public Double getRatioOfExpiredQueriesOfLeft() {
		return ratioOfExpiredQueriesOfLeft;
	}

	public Double getRatioOfCommonQueriesInRight() {
		return ratioOfCommonQueriesInRight;
	}

	public Double getRatioOfNewQueriesInRight() {
		return ratioOfNewQueriesInRight;
	}

	public Double getMedianLatencyOfCommonQueriesInLeft() {
		return medianLatencyOfCommonQueriesInLeft;
	}

	public Double getMedianLatencyOfExpiredQueriesOfLeft() {
		return medianLatencyOfExpiredQueriesOfLeft;
	}

	public Double getMedianLatencyOfCommonQueriesInRight() {
		return medianLatencyOfCommonQueriesInRight;
	}

	public Double getMedianLatencyOfNewQueriesInRight() {
		return medianLatencyOfNewQueriesInRight;
	}

	public Double getRatioOfLatencyOfCommonQueriesInLeft() {
		return ratioOfLatencyOfCommonQueriesInLeft;
	}

	public Double getRatioOfLatencyOfExpiredQueriesOfLeft() {
		return ratioOfLatencyOfExpiredQueriesOfLeft;
	}

	public Double getRatioOfLatencyOfCommonQueriesInRight() {
		return ratioOfLatencyOfCommonQueriesInRight;
	}

	public Double getRatioOfLatencyOfNewQueriesInRight() {
		return ratioOfLatencyOfNewQueriesInRight;
	}

	public ClusteredWindow getLeftClusteredWindow() {
		return leftClusteredWindow;
	}

	public ClusteredWindow getRightClusteredWindow() {
		return rightClusteredWindow;
	}

	public int getHowManyPairsAreRepresentedByThisObject() {
		return howManyPairsAreRepresentedByThisObject;
	}

	@Override
	public int compareTo(DistributionDistance o) {
		return this.showSummary().compareTo(o.showSummary());
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
		
}
