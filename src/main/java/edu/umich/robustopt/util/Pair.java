package edu.umich.robustopt.util;

import java.io.Serializable;

public class Pair<K,V> implements Serializable{

	public final K first;
	public K getKey() { return first; }

	public final V second;
	public V getValue() { return second; }

	public Pair(K key,V value) {
		this.first = key;
		this.second = value;
	}

	@Override
	public String toString() {
		return first + "=" + second;
	}

	@Override
	public int hashCode() {
		// name's hashCode is multiplied by an arbitrary prime number (13)
		// in order to make sure there is a difference in the hashCode between
		// these two parameters:
		//  name: a  value: aa
		//  name: aa value: a
		return first.hashCode() * 13 + (second == null ? 0 : second.hashCode());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o instanceof Pair) {
			Pair pair = (Pair) o;
			if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
			if (second != null ? !second.equals(pair.second) : pair.second != null) return false;
			return true;
		}
		return false;
	}
}
