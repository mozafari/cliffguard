package edu.umich.robustopt.physicalstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.vertica.VerticaDeployedProjection;

public abstract class DeployedPhysicalStructure implements Serializable, Cloneable {
	private static final long serialVersionUID = 9203734444429007381L;
	private final String schema;
	private final String name; // node-local name
	private final String basename;
	private final PhysicalStructure structure;
	private Double diskSizeInGigabytes = null;

	public DeployedPhysicalStructure (String schema, String name, String basename, PhysicalStructure structure) {
		this.schema = schema;
		this.name = name;
		this.basename = basename;
		this.structure = structure;
		assert (name!=null && structure !=null);
	}
 	
	/*
	 * Here we should not clone and need to return a soft copy! This is to make sure that there is only one 
	 * instance of each physical structure in the entire database!
	 */
	public PhysicalStructure getStructure() {
		return structure;
	}
	
	
	/**
	 * E.g., projection_schema.projection_basename (logical name)
	 * @return
	 */
	public NamedIdentifier getStructureIdent() {
		return new NamedIdentifier(getSchema(), getBasename());
	}
	
	public String getSchema() {
		return schema;
	}

	public String getName() {
		return name;
	}

	public String getBasename() {
		return basename;
	}
	
	
	public abstract DeployedPhysicalStructure clone();

	public boolean structurallyEquals(DeployedPhysicalStructure that) {
		return this.structure.equals(that.structure);
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((basename == null) ? 0 : basename.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		result = prime * result
				+ ((structure == null) ? 0 : structure.hashCode());
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
		DeployedPhysicalStructure other = (DeployedPhysicalStructure) obj;
		if (basename == null) {
			if (other.basename != null)
				return false;
		} else if (!basename.equals(other.basename))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (structure == null) {
			if (other.structure != null)
				return false;
		} else if (!structure.equals(other.structure))
			return false;
		return true;
	}

	@Override
	public String toString() {
		String structStr = getStructure().toString();
		String result = "PhysicalStructure [type=" + this.getClass().getName()  
				+ ", schema=" + getSchema()
				+ ", name=" + getName()
				+ ", basename=" + getBasename()
				+ ", structure=" + structStr + "]";
		return result;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public Double getDiskSizeInGigabytes() {
		return diskSizeInGigabytes;
	}

}
