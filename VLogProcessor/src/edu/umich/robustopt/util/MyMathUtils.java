package edu.umich.robustopt.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math.stat.StatUtils;

public class MyMathUtils {

	//Important: 0<p<=100, make sure you don't call this function with a [0, 1] value!
	public static Long percentile(List<Long> data, double p) {
		Double[] double_array = new Double[data.size()];
		for (int i=0; i<data.size(); ++i)
			double_array[i] = (double)(long)data.get(i);

		Long perc = (long)(double)percentile(double_array, p);
		return perc;
	}
	
	//Important: 0<p<=100, make sure you don't call this function with a [0, 1] value!
	public static Double percentile(Double[] data, double p) {
		boolean hasNull = false;
		for (Double d : data) {
			if (d == null) {
				hasNull = true;
				break;
			}
		}
		if (hasNull)
			return Double.NaN;
		
		double localData[] = new double[data.length];
		for (int i=0; i<data.length; ++i)
			localData[i] = data[i];
		
		Double result = StatUtils.percentile(localData, p);
		
		return result;
	}

	public static Double sum(Double[] data) {
		boolean hasNull = false;
		boolean hasInfinite = false;
		for (Double d : data) {
			if (d == null || d==Double.NaN) {
				hasNull = true;
				break;
			}
			if (d == Double.MAX_VALUE || d == Double.POSITIVE_INFINITY) {
				hasInfinite = true;
				break;
			}
		}
		if (hasNull)
			return Double.NaN;
		if (hasInfinite)
			return Double.MAX_VALUE;
		
		double localData[] = new double[data.length];
		for (int i=0; i<data.length; ++i)
			localData[i] = data[i];
		
		Double result = StatUtils.sum(localData);
		
		return result;
	}

	public static Double weightedAvg(int n1, Double v1, int n2, Double v2) {
		Double result;
		
		if (Double.isNaN(v1) && Double.isNaN(v2))
			result = Double.NaN;
		else if (Double.isNaN(v1))
			result = v2;
		else if (Double.isNaN(v2))
			result = v1;
		else //neither one is NaN
			result = (n1*v1+n2*v2) / (n1+n2);
		return result;
	}	

	/*
	public static Double addition(Double v1, Double v2) {
		Double result;
		if (Double.isNaN(v1) || Double.isNaN(v2))
			result = Double.NaN;
		else
			result = v1+v2;
		
		return result;
	}	
	*/
	
	public static double[] convertLongListToDoubleArray (List<Long> numbers) {
		double[] converted = new double[numbers.size()];
		for (int i=0; i<numbers.size(); ++i) {
			Long n = numbers.get(i);
			converted[i] = (n == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : n);
		}
		return converted;
	}
	
	public static List<Long> convertIntListToLongList (List<Integer> numbers) {
		List<Long> ok = new ArrayList<Long>();
		for (int i=0; i<numbers.size(); ++i)
			ok.add((long)numbers.get(i));
		
		return ok;
	}

	
	public static long getMeanInts(List<Integer> numbers) {
		return getMeanLongs(convertIntListToLongList(numbers));
	}
	
	public static long getMeanLongs(List<Long> numbers) {
		double[] converted = convertLongListToDoubleArray(numbers);
		boolean hasPosInf = false, hasNegInf = false, hasNan = false;
		double sum = 0.0;
		for (int i=0; i<converted.length; ++i) {
			sum += converted[i];
		}
		double mean = sum / converted.length;
		long lmean;
		if (Double.isInfinite(mean) && mean>0)
			lmean = Long.MAX_VALUE;
		else 
			lmean = (long)mean;
		
		return lmean;
	}
	
	public static long getMinInts(List<Integer> numbers) {
		return getMinLongs(convertIntListToLongList(numbers));
	}
	
	public static long getMinLongs(List<Long> numbers) {
		double[] converted = convertLongListToDoubleArray(numbers);
		double min = StatUtils.min(converted);
		return (long)min;
	}

	public static long getMaxInts(List<Integer> numbers) {
		return getMaxLongs(convertIntListToLongList(numbers));
	}
	
	public static long getMaxLongs(List<Long> numbers) {
		double[] converted = convertLongListToDoubleArray(numbers);
		double max = StatUtils.max(converted);
		return (long) max;		
	}

	public static long getStdInts(List<Integer> numbers) {
		return getStdLongs(convertIntListToLongList(numbers));
	}
	
	public static long getStdLongs(List<Long> numbers) {
		double[] converted = convertLongListToDoubleArray(numbers);
		long std;
		double range = StatUtils.max(converted) - StatUtils.min(converted);
		std = ((Double.isInfinite(range) && range>0) || Double.isNaN(range) ? Long.MAX_VALUE : (long)Math.sqrt(StatUtils.variance(converted)));
		return std;
	}
	
}
