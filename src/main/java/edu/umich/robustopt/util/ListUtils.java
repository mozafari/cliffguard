package edu.umich.robustopt.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.robustopt.common.Randomness;


public class ListUtils {
	public static <A, B> List<Pair<A, B>> Zip(List<A> a, List<B> b) {
		List<Pair<A, B>> ret = new ArrayList<Pair<A, B>>();
		int m = Math.min(a.size(), b.size());
		for (int i = 0; i < m; i++)
			ret.add(new Pair<A, B>(a.get(i), b.get(i)));
		return ret;
	}
	public static <T> List<T> PickWithoutReplacement(List<T> elems, int n) {
		if (elems.size() < n)
			throw new IllegalArgumentException("can't draw n");
		Set<Integer> seenBefore = new HashSet<Integer>();
		List<T> samples = new ArrayList<T>();
		for (int i = 0; i < n; i++) {
			boolean quit = false;
			do {
				int p = Randomness.randGen.nextInt(elems.size());
				if (seenBefore.contains(p))
					continue;
				seenBefore.add(p);
				samples.add(elems.get(p));
				quit = true;
			} while (!quit);
		}
		return samples;
	}
	/**
	 * More efficient than PickWithoutReplacement(elems, 1)
	 * 
	 * @param elems
	 * @return
	 */
	public static <T> T PickOne(List<T> elems) {
		if (elems.isEmpty())
			throw new IllegalArgumentException("can't pick empty");
		return elems.get(Randomness.randGen.nextInt(elems.size()));
	}
}
