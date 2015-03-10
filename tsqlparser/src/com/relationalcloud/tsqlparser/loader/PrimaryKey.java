package com.relationalcloud.tsqlparser.loader;

import java.util.Iterator;
import java.util.Vector;


public class PrimaryKey extends IntegrityConstraint {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Vector<String> fields = null;
	
	public PrimaryKey(String id, Vector<String> fields){
		setId(id);
		this.setFields(fields);
		
	}

	public PrimaryKey(PrimaryKey primaryKey) {
		setId(primaryKey.getId());
		setFields((Vector<String>) primaryKey.fields.clone());
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(Vector<String> fields) {
		this.fields = fields;
	}

	/**
	 * @return the fields
	 */
	public Vector<String> getFields() {
		return fields;
	}
	
	@Override
	public String toString(){
		return "pk:" + getId() + " " + fields.toString();
		
	}
	
	/**
	 * Check whether a give column is part of the Primary Key
	 * @param column
	 * @return
	 */
	public boolean isKey(String column){
		
		Iterator<String> it = fields.iterator();
		while(it.hasNext())
			if(it.next().equals(column))
				return true;
		
		return false;
	}

	public boolean sameFields(PrimaryKey primaryKey) {
		
		for(String col : primaryKey.getFields()){
			if(!this.fields.contains(col))
				return false;
		}
		
		for(String col : getFields()){
			if(!primaryKey.fields.contains(col))
				return false;
		}

		return true;
	}
	
	@Override
	public PrimaryKey clone(){

		return new PrimaryKey(this);
		
	}
	
	
	@Override
	public boolean equals(IntegrityConstraint ic){
		
		if(!(ic instanceof PrimaryKey))
			return false;

		for(int i = 0; i< fields.size(); i++){		
			if(!fields.elementAt(i).equals(((PrimaryKey)ic).fields.elementAt(i)))
				return false;
		}
		
		return true;
				
	}
			
	
}
