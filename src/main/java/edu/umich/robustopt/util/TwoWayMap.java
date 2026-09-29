package edu.umich.robustopt.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TwoWayMap<K1, K2, V> extends HashMap<K1, HashMap<K2,V>> implements Serializable {
	private static final long serialVersionUID = 7518952823365430918L;

	@Override
	public HashMap<K2,V> put(K1 key, HashMap<K2,V> value) {
		HashMap<K2,V> curValue;
		if (super.containsKey(key)) {
			curValue = super.get(key);
			curValue.putAll(value);
		} else
			curValue = value;
		return super.put(key, curValue);
	}
	
	@Override
	public void putAll(Map t) {
		for (Object keyObj : t.keySet()) {
			Object valObj = t.get(keyObj);
			put((K1)keyObj, (HashMap<K2,V>)(valObj));
		}
	}

	
}

