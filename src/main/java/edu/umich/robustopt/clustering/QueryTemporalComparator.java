package edu.umich.robustopt.clustering;

import java.util.Comparator;

public class QueryTemporalComparator implements Comparator<Query> {

	@Override
	public int compare(Query q1, Query q2) {
        try {
    		if (q1.getTimestamp() == null || q2.getTimestamp() == null)
    			System.err.println("NULL timestamp in your queries!");

			return q1.getTimestamp().compareTo(q2.getTimestamp());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // we should not get here!
        return 0;
	}

}
