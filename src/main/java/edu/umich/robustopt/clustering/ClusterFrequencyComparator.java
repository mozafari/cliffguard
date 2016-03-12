package edu.umich.robustopt.clustering;

import java.util.Comparator;

public class ClusterFrequencyComparator implements Comparator<Cluster> {

	@Override
	public int compare(Cluster c1, Cluster c2){
        try {
			if (c1.getFrequency() > c2.getFrequency())
				return -1;
			else if (c1.getFrequency() == c2.getFrequency())
				return 0;
			else
				return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // we should not get here!
        return 0;
	}

}
