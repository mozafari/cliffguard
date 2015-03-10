/* ================================================================
 * JSQLParser : java based sql parser 
 * ================================================================
 *
 * Project Info:  http://jsqlparser.sourceforge.net
 * Project Lead:  Leonardo Francalanci (leoonardoo@yahoo.it);
 *
 * (C) Copyright 2004, by Leonardo Francalanci
 *
 * This library is free software; you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation;
 * either version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307, USA.
 */

package com.relationalcloud.tsqlparser.statement.alter.table;

import java.util.List;

import com.relationalcloud.tsqlparser.schema.Index;



/**
 * An ADD CONSTRAINT alter table statement 
 * Syntax:
 * <ul>
 * 	<li>ADD {INDEX|KEY} [index_name][index_type] (index_col_name,...) [index_option] ...</li>
 * 	<li>ADD [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name][index_type] (index_col_name,...) [index_option] ...</li>
 *	<li>ADD [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...) [index_option] ...</li>
 * 	<li>ADD FULLTEXT [INDEX|KEY] [index_name] (index_col_name,...) [index_option] ...</li>
 * 	<li>ADD SPATIAL [INDEX|KEY] [index_name] (index_col_name,...) [index_option] ...</li>
 * 	<li>ADD [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...)reference_definition</li>
 * </ul>
 * @author fangar
 *
 */
public class AlterTableAddConstraintStatement  extends AlterTableStatement{

	
	//replace the fields below
	private Index index; 
	
	private String constraintWidth;
	private String constraintType;
	private String constraintName;
	private String indexType; // available if the constraint is an index
	private String indexOption; // only one option is possible 
	private List columnList;
	
	public String getConstraintWidth() {
		return constraintWidth;
	}
	public void setConstraintWidth(String constraintWidth) {
		this.constraintWidth = constraintWidth;
	}
	public String getConstraintType() {
		return constraintType;
	}
	public void setConstraintType(String constraintType) {
		this.constraintType = constraintType;
	}
	public String getConstraintName() {
		return constraintName;
	}
	public void setConstraintName(String constraintName) {
		this.constraintName = constraintName;
	}
	public String getIndexType() {
		return indexType;
	}
	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}
	public List getColumnList() {
		return columnList;
	}
	public void setColumnList(List columnList) {
		this.columnList = columnList;
	}
	public String getIndexOption() {
		return indexOption;
	}
	public void setIndexOption(String indexOption) {
		this.indexOption = indexOption;
	}
	
	public Index getIndex() {
		return index;
	}
	public void setIndex(Index index) {
		this.index = index;
	}
	
	
}
