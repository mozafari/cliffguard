package edu.umich.robustopt.physicalstructures;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public abstract class PhysicalStructure implements Serializable, Cloneable {
	private static final long serialVersionUID = -7287780051600870156L;
	private Double diskSizeInGigabytes = null;

	public Double getDiskSizeInGigabytes() {
		return diskSizeInGigabytes;
	}

	public void setDiskSizeInGigabytes(Double diskSizeInGigabytes) {
		this.diskSizeInGigabytes = diskSizeInGigabytes;
	}

	public abstract boolean equals(Object obj);

	public abstract int hashCode();

	@Override
	public PhysicalStructure clone() throws CloneNotSupportedException {		
	    try {
	        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
	        ObjectOutputStream out = new ObjectOutputStream(bOut);
	        out.writeObject(this);
	        out.close();

	        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bOut.toByteArray()));
	        PhysicalStructure copy = (PhysicalStructure)in.readObject();
	        in.close();

	        return copy;
	    }
	    catch (Exception e) {
	        throw new RuntimeException(e);
	    }		
	}
	
	public abstract ArrayList<String> createPhysicalStructureSQL(String structureName) throws Exception;
	
	public abstract String getHumanReadableSummary();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
