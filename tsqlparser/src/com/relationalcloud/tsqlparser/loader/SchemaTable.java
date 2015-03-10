package com.relationalcloud.tsqlparser.loader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;


/**
 * This class represent a relational table. 
 * 
 * @author Carlo A. Curino (carlo@curino.us)
 *
 */
public class SchemaTable implements Serializable {
	static final long serialVersionUID = 0;
	protected String schemaName;
	protected String tableName;
	private Vector<String> columns;
	private Vector<String> colTypes;
    private Vector<IntegrityConstraint> constraints = null;

	public SchemaTable(String schemaName, String tableName) {
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.setColumns(new Vector<String>());
		this.setColTypes(new Vector<String>());
        this.setConstraints(new Vector<IntegrityConstraint>());

	}
	
	public SchemaTable(SchemaTable srcTable) {
		this.tableName = srcTable.getTableName();
		this.schemaName = srcTable.getSchemaName();
		this.setColumns(new Vector<String>());
		this.setColTypes(new Vector<String>());
		
		if(srcTable.getColumns().size()!= srcTable.getColTypes().size()){
				System.err.println("TABLE COLUMNS AND TYPES MISMATCH FOR TABLE:" + srcTable.getTableName());
		}
		
		for(int i=0; i<srcTable.getColumns().size(); i++){ 
			this.getColumns().add(new String(srcTable.getColumns().get(i)));
			if(srcTable.getColTypes().get(i)==null){
				System.err.println("NULL TYPE IN TABLE -> SET DEFAULT:" + srcTable);
				this.getColTypes().add(new String("varchar(255)"));
			}
			else	
				this.getColTypes().add(new String(srcTable.getColTypes().get(i)));
		}
	    constraints = new Vector<IntegrityConstraint>();
        for(IntegrityConstraint ic :srcTable.getConstraints()){
            this.constraints.add(ic.clone());
        }
    
	}
	
	@Override
	public SchemaTable clone() {
		return new SchemaTable(this);
	}

	public void addColumn(String columnName) {
		getColumns().add(columnName);
		getColTypes().add("varchar(255)");

	}
	
	public void addColumn(String columnName, String columnType) {
		getColumns().add(columnName);
		getColTypes().add(columnType);
	}
	
	public int getNumColumns() {
		return getColumns().size();
	}
	public String getColumn(int index) {
		return getColumns().elementAt(index);
	}
	
	public Vector<String> getColumns() {
		return columns;
	}
	public int indexOf(String columnName) {
		for(int i=0; i<getNumColumns(); i++)
			if(getColumns().get(i).contentEquals(columnName))
				return i;
		return -1;
			
	}
	
	public String getColumnByName(String colname){
		if(indexOf(colname)<0)		
			return null;
		else
			return getColumns().get(indexOf(colname));
	}
	
	@Override
	public String toString() {
		String str = getTableName() + " (";
		int i;

		for(i=0; i<getNumColumns()-1; i++)
			str += getColumn(i) + ", ";
		if(getNumColumns() > 0)
			str += getColumn(i);

		str += ")";

		return str + "\n";
	}
	
	
	public boolean hasSameSignature(SchemaTable table2) {
		if(this.getNumColumns() != table2.getNumColumns())
			return false;
		for(int i=0; i<this.getNumColumns(); i++) {
			if(!this.getColumn(i).contentEquals(table2.getColumn(i)) || !this.getColType(i).contentEquals(table2.getColType(i)))
				return false;
		}
		return true;
	}
	
	public String getColType(int i) {		
		return getColTypes().get(i);
	}
	
	
	public String getColType(String columnName) {
		for(int i=0; i<this.getNumColumns(); i++) {
			if(this.getColumn(i).contentEquals(columnName))
				return getColTypes().get(i);
		}
		return null; 
	}
	
	public boolean hasColumn(String columnName) {
		for(int i=0; i<this.getNumColumns(); i++) {
			if(this.getColumn(i).contentEquals(columnName))
				return true;
		}
		return false;
	}
	
	   public void addConstraint(Vector<IntegrityConstraint> iclist) throws IntegrityConstraintsExistsException{
	        for(IntegrityConstraint ic : iclist){
	            addConstraint(ic);
	        }
	    }
	    
	    public void addConstraint(IntegrityConstraint ic) throws IntegrityConstraintsExistsException{
	        
	        for(IntegrityConstraint c : constraints){
	            if(c!=null && c.getId().equals(ic.getId()))
	                throw new IntegrityConstraintsExistsException("A constraint " + ic.getId() + " already exists in this table");
	        }
	        constraints.add(ic);
	    }

	
	public Vector<String> getColTypes() {
		return colTypes;
	}
	public void setColTypes(Vector<String> colTypes) {
		this.colTypes = colTypes;
	}
	
	@Override
	public boolean equals(Object object) {
		return equals((SchemaTable)object);
	}
	
	public boolean equals(SchemaTable table2) {
		if(table2 == null)
			return false;
		else if(this.getColumns().size() != table2.getColumns().size())
			return false;
		else {
			for(int i=0; i<this.getColumns().size(); i++) {
				if(!table2.hasColumn(this.getColumns().get(i)))
					return false;
			}
		}
		
		// passed all test
		return true;
	}
	
	
	
	public String getColumnsString() {
		String out = "";
		Iterator<String> c = this.getColumns().iterator();
		
		while(c.hasNext()){
			String temp = c.next();
			out+= temp;
			if(c.hasNext() && temp!=null)
				out+=",";
		}
		return out;
	}
	public String getColumnsStringMars() {
		String out = "";
		Iterator<String> c = this.getColumns().iterator();
		int index = 0;
		while(c.hasNext()){
			String temp = c.next();
			out+= "S" + index + "= t." + temp;
			if(c.hasNext() && temp!=null)
				out+=",";
			index++;
		}
		return out;
	}
	
		/**
	 * remove a column and its type
	 * @param colName
	 */
	public void removeColumn(String colName)  {
		if(getColumns().indexOf(colName)>=0){
			getColTypes().remove(getColumns().indexOf(colName));
			getColumns().remove(getColumns().indexOf(colName));
		}
		
		//FIXME add throws NonExistingColumnException, ConstrainedColumnException
	}

	public Vector<String> getPrimaryKey(){
		
		for(IntegrityConstraint ic:getConstraints())
			if(ic instanceof PrimaryKey){
				return ((PrimaryKey) ic).getFields();
			}
		return null;
		
	}
	
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(Vector<String> columns) {
		this.columns = columns;
	}


	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the schemaName
	 */
	public String getSchemaName() {
		return schemaName;
	}
	
	public boolean orderColumns(){
		
		for(int i=0; i< columns.size();i++)
			for(int j=0; j< columns.size();j++){
				if(columns.get(i).compareTo(columns.get(j)) < 0){
	
					String tempcol = columns.get(i);
					columns.set(i,columns.get(j));
					columns.set(j, tempcol);
			
					String temptype = colTypes.get(i);
					colTypes.set(i, colTypes.get(j));
					colTypes.set(j, temptype);
					
				}
			}
				
		return true;	
		
	}

  public void setConstraints(Vector<IntegrityConstraint> constraints) {
    this.constraints = constraints;
  }

  public Vector<IntegrityConstraint> getConstraints() {
    return constraints;
  }
	
}
