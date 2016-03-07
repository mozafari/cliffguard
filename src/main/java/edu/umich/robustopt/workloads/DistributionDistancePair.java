package edu.umich.robustopt.workloads;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.stat.StatUtils;

import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.util.MyMathUtils;

public class DistributionDistancePair extends DistributionDistance implements Serializable {
	private static final long serialVersionUID = -3467554675209511957L;
	
	private Double relativeFrequenyChangeOfNonExpiringClusters; //previously known as d1
	private Double fractionOfExpiringClusters; // previously known as d2
	private Integer howManyPairsAreRepresentedByThisObject = 0;
	final boolean ignoreDistancesWithAnyNULL = true;
	
	public DistributionDistancePair(Double relativeFrequenyChangeOfNonExpiringClusters, Double fractionOfExpiringClusters, Integer howManyPairsAreRepresentedByThisObject) {
		checkValidity(relativeFrequenyChangeOfNonExpiringClusters, fractionOfExpiringClusters);
		
		this.fractionOfExpiringClusters = fractionOfExpiringClusters;
		this.relativeFrequenyChangeOfNonExpiringClusters = relativeFrequenyChangeOfNonExpiringClusters;
		this.howManyPairsAreRepresentedByThisObject = howManyPairsAreRepresentedByThisObject;
	}
	
	public DistributionDistancePair(DistributionDistance_ClusterFrequency detailedDistance) {
		double relativeFrequenyChangeOfNonExpiringClusters = detailedDistance.getAvgRelativeProbabilityChangeForNonZerosInBothWindows();
		double fractionOfExpiringClusters = detailedDistance.getNumberOfExpiredClusters() / (double)detailedDistance.getNumberOfClustersLeft();

		checkValidity(relativeFrequenyChangeOfNonExpiringClusters, fractionOfExpiringClusters);
		this.fractionOfExpiringClusters = fractionOfExpiringClusters;
		this.relativeFrequenyChangeOfNonExpiringClusters = relativeFrequenyChangeOfNonExpiringClusters;
	}
	
	private void checkValidity(Double relativeFrequenyChangeOfNonExpiringClusters, Double fractionOfExpiringClusters) {
		if (relativeFrequenyChangeOfNonExpiringClusters < 0.0) // note that relativeFrequenyChangeOfNonExpiringClusters can be larger than 1, e.g. if its freq goes from 0.1 to 0.3 then it's relative change is 2.0
			throw new IllegalArgumentException("d1: " + relativeFrequenyChangeOfNonExpiringClusters);
		if (fractionOfExpiringClusters < 0.0 || fractionOfExpiringClusters > 1.0 || fractionOfExpiringClusters == Double.NaN)
			throw new IllegalArgumentException("d2: " + fractionOfExpiringClusters);
	}
	
	@Override
	public DistributionDistance computeAverage(DistributionDistance firstD, DistributionDistance secondD) throws Exception {
		if (!(firstD instanceof DistributionDistancePair) || !(secondD instanceof DistributionDistancePair))
			throw new Exception("Cannot average incompatible types of distances: " + firstD.getClass().getCanonicalName() + " and "+ secondD.getClass().getCanonicalName());
		
		DistributionDistancePair first = (DistributionDistancePair) firstD;
		DistributionDistancePair second = (DistributionDistancePair) secondD;
		int n1 = first.howManyPairsAreRepresentedByThisObject;
		int n2 = second.howManyPairsAreRepresentedByThisObject;
		
		if (first.fractionOfExpiringClusters==Double.NaN || second.fractionOfExpiringClusters==Double.NaN)
			throw new Exception("You cannnot have NaN as fraction of expiring clusters!");
		
		Double fractionOfExpiringClusters, relativeFrequenyChangeOfNonExpiringClusters;
		Integer howManyPairsAreRepresentedByThisObject;
		if (ignoreDistancesWithAnyNULL) {
			if (first.relativeFrequenyChangeOfNonExpiringClusters == Double.NaN) {
				fractionOfExpiringClusters = second.fractionOfExpiringClusters;
				relativeFrequenyChangeOfNonExpiringClusters = second.relativeFrequenyChangeOfNonExpiringClusters;
				howManyPairsAreRepresentedByThisObject = n2;
			} else if (second.relativeFrequenyChangeOfNonExpiringClusters == Double.NaN) {
				fractionOfExpiringClusters = first.fractionOfExpiringClusters;
				relativeFrequenyChangeOfNonExpiringClusters = first.relativeFrequenyChangeOfNonExpiringClusters;
				howManyPairsAreRepresentedByThisObject = n1;
			} else { // neither of them is NaN!
				fractionOfExpiringClusters = MyMathUtils.weightedAvg(n1, first.fractionOfExpiringClusters, n2, second.fractionOfExpiringClusters);
				relativeFrequenyChangeOfNonExpiringClusters = MyMathUtils.weightedAvg(n1, first.relativeFrequenyChangeOfNonExpiringClusters, n2, second.relativeFrequenyChangeOfNonExpiringClusters);
				howManyPairsAreRepresentedByThisObject = n1 + n2;
			}
		} else { // we are not to ignore anyone!
			fractionOfExpiringClusters = MyMathUtils.weightedAvg(n1, first.fractionOfExpiringClusters, n2, second.fractionOfExpiringClusters);
			relativeFrequenyChangeOfNonExpiringClusters = MyMathUtils.weightedAvg(n1, first.relativeFrequenyChangeOfNonExpiringClusters, n2, second.relativeFrequenyChangeOfNonExpiringClusters);
			howManyPairsAreRepresentedByThisObject = n1 + n2;
		}
			
		DistributionDistancePair avg = new DistributionDistancePair(relativeFrequenyChangeOfNonExpiringClusters, fractionOfExpiringClusters, howManyPairsAreRepresentedByThisObject);
		
		return avg;
	}

	@Override
	public String showSummary() {
		return "<relativeFrequenyChangeOfNonExpiringClusters: " + relativeFrequenyChangeOfNonExpiringClusters +", fractionOfExpiringClusters: " + fractionOfExpiringClusters + ">";
	}

	/*
	@Override
	public String toString() {
		return relativeFrequenyChangeOfNonExpiringClusters + "_" + fractionOfExpiringClusters;
	} 
	*/
	
	@Override
	public int compareTo(DistributionDistance o) {
		return this.showSummary().compareTo(o.showSummary());
	}

	public Double getFractionOfExpiringClusters() {
		return fractionOfExpiringClusters;
	}

	public Double getRelativeFrequenyChangeOfNonExpiringClusters() {
		return relativeFrequenyChangeOfNonExpiringClusters;
	}

	public static class Generator implements DistributionDistanceGenerator<DistributionDistancePair> {
		@Override
		public DistributionDistancePair distance(List<Query> leftWindow, List<Query> rightWindow) throws Exception {
			DistributionDistance_ClusterFrequency dist = new DistributionDistance_ClusterFrequency.Generator().distance(leftWindow, rightWindow);

			// We now start making all the measurements!
			double relativeFrequenyChangeOfNonExpiringClusters = dist.getAvgRelativeProbabilityChangeForNonZerosInBothWindows();
			double fractionOfExpiringClusters = dist.getNumberOfExpiredClusters() / (double)dist.getNumberOfClustersLeft();

			DistributionDistancePair result = new DistributionDistancePair(relativeFrequenyChangeOfNonExpiringClusters, fractionOfExpiringClusters, dist.getHowManyPairsAreRepresentedByThisObject());
			return result;
		}

	}
}
