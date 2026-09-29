package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.stat.StatUtils;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.common.Randomness;

public abstract class SyntheticDistributionDistancePairWorkloadGenerator extends Synthetic_SWGO_WorkloadGenerator<DistributionDistancePair>{
	public enum FrequencyDistribution {
		PowerLaw,
		Uniform
	}
		
	public SyntheticDistributionDistancePairWorkloadGenerator(
			Map<String, Schema> schema, ConstantValueManager constManager) {
		super(schema, constManager);
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Generate a random Window, used to bootstrap the process
	 * @return
	 * @throws Exception 
	 */
	public ClusteredWindow GenerateRandomWindow(int numberOfClustersPerWindow, int totalNumberOfQueriesPerWindow, FrequencyDistribution freqDist) throws Exception {
		List<Integer> frequencies;
		if (freqDist == FrequencyDistribution.PowerLaw)
			frequencies = createPowerLawFrequencies(numberOfClustersPerWindow, totalNumberOfQueriesPerWindow);
		else
			throw new Exception("Unsupported frequency distribution: " + freqDist);
				
		List<Cluster> clusterList = new ArrayList<Cluster>();
		for (int c = 0; c < frequencies.size(); ++c) {
			Query_SWGO query = GenerateRandomQuery();
			Cluster cluster = new Cluster(query);
			Cluster newCluster = createClusterWithNewFrequency(cluster, frequencies.get(c));
			clusterList.add(newCluster);
		}
		
			//debug info
			System.out.print("Generated a rnd win, 10 freqs=");
			for (int i = 0; i < (frequencies.size()>10? 10 : frequencies.size()); i++)
				System.out.print(frequencies.get(i)+", ");
			System.out.println("");
				
		return new ClusteredWindow(clusterList);
	}
	
	public static List<Integer> createPowerLawFrequencies(int NumClustersPerWindow, int NumQueriesPerWindow) {
		double[] values = new double[NumClustersPerWindow];
		for (int i = 0; i < values.length; i++) {
			values[i] = Randomness.randGen.nextDouble() * 100 + Math.exp(0.5 * i);		
		}
		// normalize values to compute frequencies
		values = Normalize(values);

		List<Integer> freqs = new ArrayList<Integer>();
		for (int i = 0; i < values.length; i++)  
			freqs.add((int) Math.ceil(values[i] * NumQueriesPerWindow));
		
		return freqs;
	}
	
	private static double[] Normalize(double[] values) {
		if (values == null)
			return null;
		double s = StatUtils.sum(values);
		double[] ret = new double[values.length];
		for (int i = 0; i < values.length; i++)
			ret[i] = values[i] / s;
		return ret;
	}

	/*
	 * This method does can handle any given window with arbitrary number of clusters, but it just doesn't change the number of clusters in the given window!
	 */
	@Override
	public ClusteredWindow forecastNextWindow(ClusteredWindow curWindow, DistributionDistancePair distancePair)
			throws Exception {
		if (distancePair==null)
			throw new Exception("You need to provide a distancePair");
		
		int maximum_number_of_attempts = 100;
		for (int attempt=0; attempt < maximum_number_of_attempts; ++attempt) {
			// perturb all non-zero fraction clusters relatively by -d1/+d1, i.e. new = old * (1 +/- d1)
			//second, pick d2 fraction of clusters and replace them with new clusters
			
			if (curWindow == null) 
				throw new Exception("We cannot forecast the next window from a null window!");
			int TotalQueriesWindow = curWindow.totalNumberOfQueries();
	
			// initialize
			List<Integer> remainingClusterIds = new ArrayList<Integer>();
			List<Integer> expiringClusterIds = new ArrayList<Integer>();			
			List<Integer> allIds = new ArrayList<Integer>();
			// we will use this map later to match the ids back to the actual clusters!
			Map<Integer, Cluster> indexToClusterMap = new HashMap<Integer, Cluster>();
			
			for (Cluster c : curWindow.getClusters()){
				int id = indexToClusterMap.size();
				indexToClusterMap.put(id, c);
				allIds.add(id);
			}
			double[] newFractions = new double[indexToClusterMap.size()];
			int[] oldFreqs = new int[indexToClusterMap.size()];
			
			// purturb each 
			int cc=-1;
			for (Cluster cluster : curWindow.getClusters()) {
				++cc;
				// first purturb relative to their current fraction
				double oldFrac = curWindow.getFraction(cluster); 
				oldFreqs[cc] = (int) Math.round(oldFrac * TotalQueriesWindow);
	
				if (oldFrac > 0.0) {
					Double d1 = distancePair.getRelativeFrequenyChangeOfNonExpiringClusters();
					double newFreq = oldFrac * (1 + sampleDoubleInRange(-2*d1, 2*d1) );
					if (newFreq>1.0)
						continue;
					
					newFractions[cc] = newFreq > 1.0 ? 1.0 : newFreq;
				}
			}
	
			//second, pick d2 fraction of clusters and replace them with new clusters
			//now find who does not expire!
			Double d2 = distancePair.getFractionOfExpiringClusters();
			int numberOfExpiredClusters = (int) Math.ceil(d2*indexToClusterMap.size());
			int numberOfRemainingClusters = indexToClusterMap.size() - numberOfExpiredClusters;
			double weightsCopy[] = newFractions.clone(); 
			int[] chosenIdx = weightedSampleWithoutReplacement(numberOfRemainingClusters, weightsCopy);
			
			for (int i=0; i < chosenIdx.length; ++i) {
				int chosenIndex = chosenIdx[i];
				remainingClusterIds.add(chosenIndex);
			}
			expiringClusterIds.addAll(allIds);
			expiringClusterIds.removeAll(remainingClusterIds);
			assert remainingClusterIds.size() + expiringClusterIds.size() == curWindow.getClusters().size();
			
			//now calculate the fractions for the new clusters that will replace those in expiringClusterIds
			
			double lostMass = 1.0d;
			for (Integer cId : remainingClusterIds)
				lostMass -= newFractions[cId];
			
			if (lostMass <0.0d || lostMass >1.0d)
				continue;
				//throw new Exception("Impossible configuration: " +  newFractions);
			
			int numberOfQueriesForNewClusters = (int) Math.round(lostMass * TotalQueriesWindow);
	
			//now assign numberOfQueriesForNewClusters among the clusters for which newClusters[c]==true
			if (numberOfQueriesForNewClusters < numberOfExpiredClusters)
				continue;
				//throw new Exception("Impossible configuration: numberOfQueriesForNewClusters="+numberOfQueriesForNewClusters + ", numberOfExpiredClusters="+numberOfExpiredClusters);
			
			int[] newFreqs = new int[newFractions.length];
			assert numberOfExpiredClusters >= 1;
			
			if (numberOfExpiredClusters == 1) {
				newFreqs[expiringClusterIds.get(0)] = numberOfQueriesForNewClusters;
			} else {
				int[] seperators = uniformSampleWithReplacement(0, numberOfQueriesForNewClusters-numberOfExpiredClusters+1, 
																numberOfExpiredClusters-1);
					
				Arrays.sort(seperators);
					
				//System.out.println("RAW = " + seperators);
				for (int c=0; c<expiringClusterIds.size()-1; ++c) {
					int nextExpiringClusterId = expiringClusterIds.get(c);
					if (c==0)
						newFreqs[nextExpiringClusterId] = seperators[0] + 1;
					else
						newFreqs[nextExpiringClusterId] = seperators[c] - seperators[c-1] + 1;
				}
				newFreqs[expiringClusterIds.get(expiringClusterIds.size()-1)] = numberOfQueriesForNewClusters -numberOfExpiredClusters - seperators[expiringClusterIds.size()-2] + 1;
			}
			for (Integer cId : remainingClusterIds)
				newFreqs[cId] = (int) Math.round(newFractions[cId] * TotalQueriesWindow);
						
			// now actual clusters
			List<Cluster> clusterList = new ArrayList<Cluster>();
			
			for (Integer cId : remainingClusterIds) {
				Cluster cluster = indexToClusterMap.get(cId);
				Query_SWGO q = (Query_SWGO) cluster.retrieveAQueryAtPosition(0);
				if (newFreqs[cId]>=1) {
					Cluster newCluster = createClusterWithNewFrequency(new Cluster(q), newFreqs[cId]); // this function will also set the SQL statement!
					clusterList.add(newCluster);
				} else {
					System.out.println("Mistake? unexpected zero frequency");
				}
			}
			
			for (Integer cId : expiringClusterIds) {
				Query q = GenerateRandomQuery();
				if (newFreqs[cId]>=1) {
					Cluster newCluster = createClusterWithNewFrequency(new Cluster(q), newFreqs[cId]); // this function will also set the SQL statement!
					clusterList.add(newCluster);
				} else {
					System.out.println("Mistake? unexpected zero frequency");
				}
			}
			
			return new ClusteredWindow(clusterList);
		}
		
		throw new Exception("Impossible configuration. curWindow = " + curWindow);		
	}
	
	private int[] uniformSampleWithReplacement(int lowerBoundInclusive, int upperBoundExclusive, int howMany) {
		int[] samples = new int[howMany];
		assert lowerBoundInclusive < upperBoundExclusive;
		
		/*if (lowerBoundInclusive == upperBoundExclusive) {
			for (int i=0; i<howMany; ++i)
				samples[i] = lowerBoundInclusive;
		} else*/ {	
			for (int i=0; i<howMany; ++i) {
				int range = upperBoundExclusive - lowerBoundInclusive;
				double random = Randomness.randGen.nextDouble();
				samples[i] = (int) (random * range) + lowerBoundInclusive;
			}
		}
		return samples;
	}
	
	private double sampleDoubleInRange(double a, double b) {
		if (a >= b)
			throw new IllegalArgumentException("a >= b");
		double dist = b - a;
		double x = Randomness.randGen.nextDouble();
		x *= dist; // scale x
		x += a; // bias x
		if (x < a || x > b)
			throw new RuntimeException("bad");
		return x;
	}

	private int[] weightedSampleWithoutReplacement(int howMany, double[] weights) {
				// Compute the total weight of all items together
		assert howMany <= weights.length;
		int[] chosenIdx = new int[howMany];
		
		// Now choose a random item
		int randomIndex = -1;
		for (int idx=0; idx<howMany; ++idx) {
			double totalWeight = StatUtils.sum(weights);		
			double random = Randomness.randGen.nextDouble() * totalWeight;
			for (int i = 0; i < weights.length; ++i)
			{
			    random -= weights[i];
			    if (random <= 0.0d && weights[i]>0.0d)
			    {
			        randomIndex = i;
			        break;
			    }
			}
			chosenIdx[idx] = randomIndex;
			weights[randomIndex] = 0.0d; //to ensure without replacement!
		}
		return chosenIdx;
	}
	
	@Override
	public DistributionDistanceGenerator<DistributionDistancePair> getDistributionDistanceGenerator() throws Exception {
		return new DistributionDistancePair.Generator();
	}

}
