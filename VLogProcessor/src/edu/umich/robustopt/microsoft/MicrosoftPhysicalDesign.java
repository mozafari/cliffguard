package edu.umich.robustopt.microsoft;

import java.util.List;
import java.util.Set;

import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class MicrosoftPhysicalDesign extends PhysicalDesign {

	private String physicalDesignFilename;

	public MicrosoftPhysicalDesign(Set<PhysicalStructure> physicalStructures, String physicalDesignFilename) {
		super(physicalStructures);
		this.physicalDesignFilename = physicalDesignFilename;
	}

	public MicrosoftPhysicalDesign(List<PhysicalStructure> physicalStructures, String physicalDesignFilename) {
		super(physicalStructures);
		this.physicalDesignFilename = physicalDesignFilename;
	}

	public String getPhysicalDesignFilename() {
		return physicalDesignFilename;
	}

}
