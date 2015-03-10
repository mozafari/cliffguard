package com.relationalcloud.tsqlparser.loader;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;



/**
 * This class represent the internal reresentation of the Schema, which is evolved by SMOs
 * 
 * @author Carlo A. Curino (carlo@curino.us)
 */
public class Schema implements Serializable, Comparable {
	static final long serialVersionUID = 0;
	private Vector<SchemaTable> tables;	
	public int versionID;
	protected String SCHEMA_NAME = null;
	public String CATALOG_NAME;
	public String DEFAULT_CHARACTER_SET_NAME;
	public String DEFAULT_COLLATION_NAME;
	public String SQL_PATH;
	public String DRIVER;
	
	
	public Schema(String CATALOG_NAME, String SCHEMA_NAME,
			String DEFAULT_CHARACTER_SET_NAME, String DEFAULT_COLLATION_NAME,
			String SQL_PATH, String driver) {
		super();
		this.versionID = 1;
		this.DRIVER=driver;
		this.CATALOG_NAME = CATALOG_NAME;
		this.setSchemaName(SCHEMA_NAME);
		this.DEFAULT_CHARACTER_SET_NAME = DEFAULT_CHARACTER_SET_NAME;
		this.DEFAULT_COLLATION_NAME = DEFAULT_COLLATION_NAME;
		this.SQL_PATH = SQL_PATH;

		if(SQL_PATH == null)
			SQL_PATH=" ";
	
		setTables(new Vector<SchemaTable>());
	}

	public Schema () {
		setTables(new Vector<SchemaTable>());
		versionID = -1;
	}

	public Schema (int versionID) {
		setTables(new Vector<SchemaTable>());
		this.versionID = versionID;
	}

	public Schema (Schema srcSchema) {
		this.setSchemaName(srcSchema.getSchemaName());
		this.setTables(new Vector<SchemaTable>());
		for(int i=0; i<srcSchema.size(); i++)
			this.addTable(new SchemaTable(srcSchema.getTable(i)));
		this.versionID = srcSchema.versionID;
	}

	public Schema clone() {
		return new Schema(this);
	}
	
	public void addTable(SchemaTable table) {
		getTables().add(table);
	}
	
	public SchemaTable getTable(int i) {
		return getTables().get(i);
	}
	
	public SchemaTable getTable(String tablename) {
		for(int i=0; i<getTables().size(); i++)
			if(getTables().get(i).getTableName().equalsIgnoreCase(tablename)) {
				//System.out.println("tables.get("+i+") : "+tables.get(i));
				return getTables().get(i);
			}
		//	System.out.println(tablename + "NOT FOUND");
		//	System.out.println(this.tables);
		return null;
	}
	
	/*
	 * @return all tables and columns in this schema
	 */
	public Vector<String> getAllSchemaParts() {
		Vector<String> allSchemaParts = new Vector<String>(getTables().size()*20);
		for(int i=0; i<getTables().size(); i++) {
			allSchemaParts.add(getTables().get(i).getTableName());
			for(int j=0; j<getTables().get(i).getNumColumns(); j++) {
				allSchemaParts.add(getTables().get(i).getTableName() + "." 
						+ getTables().get(i).getColumn(j));
			}
		}
		return allSchemaParts;
	}

	/*
	 * @return all tables in this schema
	 */
	public Vector<String> getAllTables() {
		Vector<String> allTables = new Vector<String>(getTables().size());
		for(int i=0; i<getTables().size(); i++) {
			allTables.add(getTables().get(i).getTableName());
		}
		return allTables;
	}

	public int indexOf(String tableName) {
		for(int i=0; i<size(); i++)
			if(getTables().get(i).getTableName().contentEquals(tableName))
				return i;
		return -1;
	}

	// check to see if this schema contains a matching table as table2
	// table name should match, column name should match, and column order should match
	public int indexOf(SchemaTable table2) {
		for(int i=0; i<getTables().size(); i++) {
			SchemaTable table = getTables().get(i);

			// test 1: table name match
			if(!table.getTableName().equals(table2.getTableName()))
				continue;
			
			// test 2: table cardinality match
			if(table.getColumns().size() != table2.getColumns().size())
				continue;
			
			// test 3: individual columns
			boolean allColumnsMatch = true;
			for(int j=0; j<table.getColumns().size(); j++) {
				if(!table.getColumns().get(j).equals(table2.getColumns().get(j))) {
					allColumnsMatch = false;
					break;
				}
			}
			if(allColumnsMatch)
				return i;
		}
		return -1;
	}
	
	public boolean containsTable(SchemaTable table2) {
		int index = indexOf(table2);
		if(index<0) 
			return false;
		else 
			return true;
	}
	
	public boolean containsTable(String tableName) {
		int index = indexOf(tableName);
		if(index<0) 
			return false;
		else 
			return true;
	}
	
	public boolean containsColumn(String tablename, String columnname) {
		boolean found = false;
		SchemaTable table = null;
				
		for(int i=0; i<getTables().size(); i++) {
			if(getTables().get(i).getTableName().contentEquals(tablename)) {
				table = getTables().get(i);
				break;
			}
		}
		
		if(table == null)			
			return false;
		else if(table.indexOf(columnname) > -1)
			return true;
		else 
			return false;
	}

	
	public int size() {
		return getTables().size();
	}
	
	public String toString() {
		return getTables().toString();
	}

	public String toStringWithMetadata() {
		return "Schema version: v" + versionID + "\n" + 
			getTables().toString();
	}


	public void setSchemaName(String schemaName) {
		this.SCHEMA_NAME = schemaName;
	}

	public String getSchemaName() {
		return SCHEMA_NAME;
	}
	
	public boolean equals(Object object) {
		return equals((Schema)object);
	}
	
	public boolean equals(Schema schema2) {

		if(schema2 == null) 
			return false; // this object is not null
		else if(this.getTables().size() != schema2.getTables().size())
			return false;
		else {
			for(int i=0; i<this.getTables().size(); i++) {
				SchemaTable table = this.getTables().get(i);
				SchemaTable table2 = schema2.getTable(table.getTableName());
				if(table2 == null) // this.table is not found in schema2
					return false;
				else if(!table.equals(table2))
					return false;
			}
		}
		
		// passed all test
		return true;
	}

	// return {diff1, diff2} - diff1 for "this" and diff2 for "schema2" 
	public Schema[] getDiffs(Schema schema2) {
		Schema diff1 = new Schema();
		Schema diff2 = new Schema();
		
		if(schema2 == null) 
			return null; // this object is not null
		else {
			for(int i=0; i<this.getTables().size(); i++) {
				SchemaTable table = this.getTables().get(i);
				SchemaTable table2 = schema2.getTable(table.getTableName());
				if(table2 == null) {// this.table is not found in schema2
					diff1.addTable(table);
				}
				else if(!table.equals(table2)) {
					diff1.addTable(table);
					diff2.addTable(table2);
				}
			}
			
			for(int i=0; i<schema2.getTables().size(); i++) {
				SchemaTable table2 = schema2.getTables().get(i);
				SchemaTable table = this.getTable(table2.getTableName());
				if(table == null) {// this.table is not found in schema2
					diff2.addTable(table2);
				}
			}
		}
		
		// passed all test
		Schema[] diffs = {diff1, diff2};
		return diffs;
	}
	
	/**
	 * 
	 * @param schema2
	 * @return true if this contains schema2
	 */
	public boolean containsSchema(Schema schema2) {
		if(schema2 == null) 
			return true;
		
		for(int i=0; i<schema2.size(); i++) {
			SchemaTable table2 = schema2.getTable(i);
			int index = this.indexOf(table2.getTableName());
			if(index < 0)
				return false;
			SchemaTable table = this.getTables().get(index);
			if(!table.getColumns().containsAll(table2.getColumns()))
				return false;
		}
		
		return true;
	}

	public int compareTo(Object o){
		Schema s = (Schema) o;
		if(!s.SCHEMA_NAME.equals(SCHEMA_NAME)){
		    System.err.println("Schema.compareTo():  The two schemas are different, no comparison is possible");
		    System.exit(-1);
		}
		if(s.versionID<this.versionID)
			return 1;
		if(s.versionID==this.versionID)
			return 0;
		
		return -1;
	}

	/**
	 * @param tables the tables to set
	 */
	public void setTables(Vector<SchemaTable> tables) {
		this.tables = tables;
	}

	/**
	 * @return the tables
	 */
	public Vector<SchemaTable> getTables() {
			return tables;
	}

	public ArrayList<SchemaTable> getTableByColumn(String columnName) {
		
		ArrayList<SchemaTable> ret = new ArrayList<SchemaTable>();
		
		for(SchemaTable s:tables)
			if(s.hasColumn(columnName))
				ret.add(s);
		return ret;
	}

	
	
}
