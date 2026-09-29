package edu.umich.robustopt.workloads;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValueDistribution implements Serializable {

	private final List<Object> values;
	private final List<Integer> counts;
	private final int sumCounts;
	
	public static final ValueDistribution DummyDistribution = new ValueDistribution(Collections.singletonList((Object)"hi"), Collections.singletonList(1)); 
	
	// sorted 
	private final List<Comparable<Object>> sortedValues;
	
	public ValueDistribution(List<Object> values, List<Integer> counts) {
		assert values.size() == counts.size();
		
		if (values.isEmpty())
			throw new IllegalArgumentException("empty vaules");
		
		this.values = values;
		this.counts = counts;
		
		int accum = 0;
		for (Integer i : counts)
			accum += i;
		this.sumCounts = accum;
		
		// XXX: this is super hacky
		this.sortedValues = new ArrayList< Comparable<Object> >();
		for (Object o : values)
			sortedValues.add((Comparable<Object>) o);
		Collections.sort(sortedValues);
	}
	
	private static boolean DoubleWithinFloatRange(double d) {
		return d >= Float.MIN_VALUE && d <= Float.MAX_VALUE;
	}
	
	public Object cdf(double d) {
		if (d < 0.0 || d > 1.0)
			throw new IllegalArgumentException("bad d");
		int n = (int) Math.floor(sumCounts * d);
		int accum = 0;
		for (int i = 0; i < counts.size(); i++) {
			accum += counts.get(i);
			if (n <= accum)
				return values.get(i);
		}
		return values.get(values.size() - 1);
	}
	
	// returns a 2 elem object array containing the next [lower, upper] bounds on o. 
	// either could be o. If o is out of the range, clamps o to the range
	public Object[] findBounds(Object o) {
		// some type coercion rules. only allow type-widening, no shortening
		Object x = sortedValues.get(0);
		if (!x.getClass().equals(o.getClass())) {
			if (x instanceof Long) {
				if (o instanceof Integer)
					o = ((Integer) o).longValue();
			} else if (x instanceof Float) {
				if (o instanceof Integer)
					o = ((Integer) o).floatValue();
				else if (o instanceof Long)
					o = ((Long) o).floatValue();
				else if (o instanceof Double) 
					o = DoubleWithinFloatRange((Double) o) ? ((Double) o).floatValue() : o;
			} else if (x instanceof Double) {
				if (o instanceof Integer)
					o = ((Integer) o).doubleValue();
				else if (o instanceof Long)
					o = ((Long) o).doubleValue();
				else if (o instanceof Float)
					o = ((Float) o).doubleValue();
			}
			if (!x.getClass().equals(o.getClass()))
				throw new RuntimeException(
					"x={" + x + "}, o={" + o + "}. Bad types: " + x.getClass().getCanonicalName() + 
					" compared to " + o.getClass().getCanonicalName());
		}
		
		Object[] ret = new Object[2];
		int pt = Collections.binarySearch(sortedValues, o);
		if (pt < 0) {
			// not found
			pt = -(pt + 1);
			if (pt == sortedValues.size())
				pt = sortedValues.size() - 1;
		}
		
		assert pt >= 0 && pt < sortedValues.size();

		// try to return [values[pt-1], values[pt+1]]
		ret[0] = pt > 0 ? sortedValues.get(pt - 1) : sortedValues.get(pt);
		ret[1] = pt < (sortedValues.size() - 1) ? sortedValues.get(pt + 1) : sortedValues.get(pt);

		return ret;
	}
}
