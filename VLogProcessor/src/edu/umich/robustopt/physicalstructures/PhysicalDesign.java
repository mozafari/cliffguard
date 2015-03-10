package edu.umich.robustopt.physicalstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class PhysicalDesign implements Serializable {
	private static final long serialVersionUID = -1836780146428511820L;
	/**
	 * @param args
	 */
	
	private Set<PhysicalStructure> physicalStructures;
	
	public PhysicalDesign(Set<PhysicalStructure> physicalStructures) {
		this.physicalStructures = physicalStructures;		
	}

	public PhysicalDesign(List<PhysicalStructure> physicalStructures) {
		this.physicalStructures = new HashSet<PhysicalStructure>();
		this.physicalStructures.addAll(physicalStructures);
	}

	public Set<PhysicalStructure> getPhysicalStructures() {		
		return Collections.unmodifiableSet(physicalStructures);
	}
	
	public List<PhysicalStructure> getPhysicalStructuresAsList() {		
		return Collections.unmodifiableList(new ArrayList(physicalStructures));
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

	public int size() {
		return physicalStructures.size();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((physicalStructures == null) ? 0 : physicalStructures
						.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PhysicalDesign other = (PhysicalDesign) obj;
		if (physicalStructures == null) {
			if (other.physicalStructures != null)
				return false;
		} else if (!physicalStructures.equals(other.physicalStructures))
			return false;
		return true;
	}

	
	
}
