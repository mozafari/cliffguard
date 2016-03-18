package edu.umich.robustopt.vertica;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class VerticaDesign extends PhysicalDesign {

	
	public VerticaDesign(Set<VerticaProjectionStructure> verticaProjections) {
		super(convertVerticaProjectionsIntoPhysicalStructures(verticaProjections));
	}
	
	public VerticaDesign(List<VerticaProjectionStructure> verticaProjections) {
		this(new HashSet<VerticaProjectionStructure>(verticaProjections));		
	}
	
	public static Set<PhysicalStructure> convertVerticaProjectionsIntoPhysicalStructures(Set<VerticaProjectionStructure> verticaProjections) {
		Set<PhysicalStructure> result = new HashSet<PhysicalStructure>();
		for (VerticaProjectionStructure struct : verticaProjections)
			result.add((PhysicalStructure)struct);
		
		return result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
