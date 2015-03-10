package edu.umich.robustopt.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

//public class SafeMap<K,V> extends ConcurrentHashMap<K, V> implements Serializable {
public class SafeMap<K,V> extends HashMap<K, V> implements Serializable {
	private static final long serialVersionUID = 7518952823365430918L;

	@Override
	public V put(K key, V value) {
		/*
		K newKey;
		if (key instanceof String && key!=null)
			newKey = (K)(((String)key).toLowerCase());
		else
			newKey = key;
		
		same here!
		*/
		return super.put(key, value);
	}
	
	@Override
	public boolean containsKey(Object key){
		/*
		Object newKey;
		if (key instanceof String)
			newKey = ((String)key).toLowerCase();
		else if (key instanceof Pair) {
			Pair<A, B> currentKey = (Pair<A,B>) key;
			if (currentKey.first instanceof String)
				currentKey.first = (A)((String)(currentKey.first)).toLowerCase();
			if (currentKey.second instanceof String)
				currentKey.second = (B)((String)(currentKey.second)).toLowerCase();
			newKey = new Pair<A, B>(currentKey.first, currentKey.second);
		} else
			newKey = key;
		*/
		return super.containsKey(key);
	}
	
}

