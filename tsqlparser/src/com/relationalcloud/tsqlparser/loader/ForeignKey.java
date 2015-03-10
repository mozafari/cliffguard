package com.relationalcloud.tsqlparser.loader;


import java.util.Iterator;
import java.util.Vector;


public class ForeignKey extends IntegrityConstraint {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Vector<String> sourceColumns = null;
	private String targetTable = null;
	private Vector<String> targetColumns = null;
	
	
	
	public ForeignKey(String fkid, Vector<String> sourceColumns, String targetTable, Vector<String> targetColumns){
		setId(fkid);
		this.sourceColumns = sourceColumns;
		this.targetTable = targetTable;
		this.targetColumns = targetColumns;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setSourceColumns(Vector<String> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}

	/**
	 * @return the fields
	 */
	public Vector<String> getSourceColumns() {
		return sourceColumns;
	}
	
	@Override
	public String toString(){
		String output =  getId() + ":(";
		for(String c : sourceColumns)
			output += c + ",";
		output = output.substring(0, output.length()-1);
		output += ")->"+targetTable+ "(";
		for(String c : targetColumns)
			output += c + ",";
		output = output.substring(0, output.length()-1);
		output += ")";
		return output;
	}
	
	/**
	 * Check whether a give column is part of the Foreign Key, as a source column
	 * @param column
	 * @return
	 */
	public boolean isSourceColumn(String column){
		
		Iterator<String> it = sourceColumns.iterator();
		while(it.hasNext())
			if(it.next().equals(column))
				return true;
		
		return false;
	}
	
	/**
	 * Check whether the input table is the target of this foreign key
	 * @param targetTable
	 * @return
	 */
			
	public boolean isTargetTable(String targetTable){
		return this.targetTable.equals(targetTable);
	}
	
	
	/**
	 * Check whether a give column is part of the Foreign Key, as a target column
	 * @param column
	 * @return
	 */
	public boolean isTargetColumn(String column){
		
		Iterator<String> it = targetColumns.iterator();
		while(it.hasNext())
			if(it.next().equals(column))
				return true;
		
		return false;
	}

	
	
	
	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public Vector<String> getTargetColumns() {
		return targetColumns;
	}

	public void setTargetColumns(Vector<String> targetColumns) {
		this.targetColumns = targetColumns;
	}

	public void renameSourceColumn(String columnName, String columnName2) {
		for(int i = 0; i< sourceColumns.size();i++){
			if(sourceColumns.elementAt(i).equals(columnName))
				sourceColumns.set(i, columnName2);	
		}
	}

	public void renameTargetColumn(String columnName, String columnName2) {
		for(int i = 0; i< targetColumns.size();i++){
			if(targetColumns.elementAt(i).equals(columnName))
				targetColumns.set(i, columnName2);	
		}
	}

	/**
	 * Verifies whether the two set of ForeignKeys are equivalent (equals in the current implementation).
	 * @param fk1list
	 * @param fk2list
	 * @return true if equals
	 */
	public static boolean equivalentForeignKeys(Vector<ForeignKey> fk1list, Vector<ForeignKey> fk2list) {
		boolean allcovered = true;
		for(ForeignKey fk1:fk1list){
			boolean covered = false;
			for(ForeignKey fk2:fk2list){
				if(fk1.getTargetTable().equals(fk2.getTargetTable()) && fk1.getSourceColumns().containsAll(fk2.getSourceColumns()) && fk1.getTargetColumns().containsAll(fk2.getTargetColumns()))
					covered = true;
			}
			if(!covered)
				allcovered = false;
		}	
		
		for(ForeignKey fk2:fk2list){
			boolean covered = false;
			
			for(ForeignKey fk1:fk1list){
				if(fk1.getTargetTable().equals(fk2.getTargetTable()) &&  fk2.getSourceColumns().containsAll(fk1.getSourceColumns()) && fk2.getTargetColumns().containsAll(fk1.getTargetColumns()))
					covered = true;
			}
			if(!covered)
				allcovered = false;
		}			
		return allcovered;
	}
	
	@Override
	public boolean equals(IntegrityConstraint ic){
		
		if(!(ic instanceof ForeignKey))
			return false;
		if(!targetTable.equals(((ForeignKey)ic).targetTable))
			return false;
		for(int i = 0; i< sourceColumns.size(); i++){		
			if(!sourceColumns.elementAt(i).equals(((ForeignKey)ic).sourceColumns.elementAt(i)))
				return false;
		}
		
		for(int i = 0; i< targetColumns.size(); i++){		
			if(!targetColumns.elementAt(i).equals(((ForeignKey)ic).targetColumns.elementAt(i)))
				return false;
		}
		return true;
				
	}
		
		
	
	
	@Override
	public ForeignKey clone(){
		
		Vector<String> sc = new Vector<String>();
		for(String s : sourceColumns){
			sc.add(s);
		}
		
		Vector<String> tc = new Vector<String>();
		for(String s : targetColumns){
			tc.add(s);
		}
		
		return new ForeignKey(this.getId(),sc, targetTable, tc);
		
	}
	
			
}
