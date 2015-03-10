package edu.umich.robustopt.vertica;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class Utils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static List<VerticaProjectionStructure> convertPhysicalStructureSetIntoVerticaStructureList(Set<PhysicalStructure> set) throws Exception {
		List<VerticaProjectionStructure> list = new ArrayList<VerticaProjectionStructure>();
		for (PhysicalStructure ps : set) 
			if (!(ps instanceof VerticaProjectionStructure))
				throw new Exception("ps=" + ps +" is not an instance of VerticaProjectionStructure");
			else
				list.add((VerticaProjectionStructure)ps);
		
		return list;		
	}
	
	public static List<PhysicalStructure> convertPhysicalStructureSetIntoPhysicalList(Set<PhysicalStructure> set) throws Exception {
		List<PhysicalStructure> list = new ArrayList<PhysicalStructure>(set);
		return list;		
	}
	
}
