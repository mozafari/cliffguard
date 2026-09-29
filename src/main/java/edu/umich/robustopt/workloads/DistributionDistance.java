package edu.umich.robustopt.workloads;

import java.util.List;

public abstract class DistributionDistance implements Comparable<DistributionDistance> {
	public abstract DistributionDistance computeAverage(DistributionDistance first, DistributionDistance second) throws Exception;

	public abstract String showSummary();
	
	@Override
	public String toString() {
		return showSummary();
	}
	
	// should return -1 if this<otherObject, 0 if this=otherObject, and +1 if this>otherObject
	@Override
	public abstract int compareTo(DistributionDistance otherObject);
		
	/*
	@Override
	public int compareTo(DistributionDistance o) {
		return this.showSummary().compareTo(o.showSummary());
	} */
	
	public static DistributionDistance computeMinimumDistance(List<DistributionDistance> distances) throws Exception {
		if (distances==null || distances.isEmpty())
			throw new Exception("computeMinimumDistance: Input distance is either null or empty: " + (distances==null));
		
		int numberOfWindows = distances.size();
		
		DistributionDistance chosenDistance = distances.get(0);
		
		if (numberOfWindows >= 2)
			for (int winId=1; winId<numberOfWindows; ++winId)	
				if (distances.get(winId).compareTo(chosenDistance) < 0)
					chosenDistance = distances.get(winId);

		return chosenDistance;		
	}

	public static DistributionDistance computeMaximumDistance(List<DistributionDistance> distances) throws Exception {
		if (distances==null || distances.isEmpty())
			throw new Exception("computeMaximumDistance: Input distance is either null or empty: " + (distances==null));
		
		int numberOfWindows = distances.size();
		
		DistributionDistance chosenDistance = distances.get(0);
		
		if (numberOfWindows >= 2)
			for (int winId=1; winId<numberOfWindows; ++winId)	
				if (distances.get(winId).compareTo(chosenDistance) > 0)
					chosenDistance = distances.get(winId);

		return chosenDistance;		
	}

	public static DistributionDistance computeAverageDistance(List<DistributionDistance> distances) throws Exception {
		if (distances==null || distances.isEmpty())
			throw new Exception("computeAverageDistance: Input distance is either null or empty: " + (distances==null));
		
		int numberOfWindows = distances.size();
		
		DistributionDistance chosenDistance = distances.get(0);
		
		if (numberOfWindows >= 2)
			for (int winId=1; winId<numberOfWindows; ++winId)	
				chosenDistance = chosenDistance.computeAverage(chosenDistance, distances.get(winId));

		return chosenDistance;		
	}

}
