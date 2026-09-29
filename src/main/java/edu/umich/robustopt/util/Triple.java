package edu.umich.robustopt.util;

import java.io.Serializable;

public class Triple <K,V,W> implements Serializable {
	private static final long serialVersionUID = -3429885654922699291L;

	public final K first;
	public final V second;
	public final W third;
	
	public Triple(K first, V second, W third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Triple))
			return false;
		Triple other = (Triple) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		if (third == null) {
			if (other.third != null)
				return false;
		} else if (!third.equals(other.third))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Triple [first=" + first + ", second=" + second + ", third="
				+ third + "]";
	}
	
	
}
