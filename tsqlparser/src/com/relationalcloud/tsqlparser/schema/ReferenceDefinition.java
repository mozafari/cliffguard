package com.relationalcloud.tsqlparser.schema;

import java.util.List;

/**
 * A reference definition object. Contains the referenced table and columns inside a foreign key
 * 
 * REFERENCES tbl_name (index_col_name,...)
 * <bR>[MATCH FULL | MATCH PARTIAL | MATCH SIMPLE]
 * <br>[ON DELETE reference_option]
 * <br>[ON UPDATE reference_option]
 * @author fangar
 *
 */
public class ReferenceDefinition {

	
	private Table tableReferenced;
	private List<String> columnsReferenced;
	private String matchType;
	private String[] referentialActions;
	
	
	public Table getTableReferenced() {
		return tableReferenced;
	}
	public void setTableReferenced(Table tableReferenced) {
		this.tableReferenced = tableReferenced;
	}
	public List<String> getColumnsReferenced() {
		return columnsReferenced;
	}
	public void setColumnsReferenced(List<String> columnsReferenced) {
		this.columnsReferenced = columnsReferenced;
	}
	public String getMatchType() {
		return matchType;
	}
	public void setMatchType(String matchType) {
		this.matchType = matchType;
	}
	public String[] getReferentialActions() {
		return referentialActions;
	}
	public void setReferentialActions(String[] referentialActions) {
		this.referentialActions = referentialActions;
	}
	
	
	
	
}
