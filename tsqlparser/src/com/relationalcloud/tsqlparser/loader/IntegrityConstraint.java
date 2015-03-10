package com.relationalcloud.tsqlparser.loader;

import java.io.Serializable;


/**
 * @author Carlo A. Curino (carlo@curino.us)
 *
 */
public abstract class IntegrityConstraint implements Serializable{
	
	private String id;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public IntegrityConstraint(){
	
		
	}
	
	@Override
	public IntegrityConstraint clone(){
		
		try {
			throw new NotImplementedException("The clone method should be implemented in the subtypes!");
		} catch (NotImplementedException e) {
			e.printStackTrace();
		}
		return null;	
	}

	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	public abstract boolean equals(IntegrityConstraint ic);
	

}
