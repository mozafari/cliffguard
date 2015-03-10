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

import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;


/**
 * A MODIFY COLUMN statement
 * Syntax:
 * MODIFY [COLUMN] col_name column_definition [FIRST | AFTER col_name]
 * @author fangar
 *
 */
public class AlterTableModifyColumnStatement extends AlterTableStatement implements Statement {

	//private Column columnModified;
	//private ColDataType newDataType;
	private String columnPosition;
	private Column columnPositionName;
	private ColumnDefinition colDef;
	//private List options;
	
	public String getColumnPosition() {
		return columnPosition;
	}

	public void setColumnPosition(String columnPosition) {
		this.columnPosition = columnPosition;
	}

	public Column getColumnPositionName() {
		return columnPositionName;
	}

	public void setColumnPositionName(Column columnPositionName) {
		this.columnPositionName = columnPositionName;
	}

	public ColumnDefinition getColDef() {
		return colDef;
	}

	public void setColDef(ColumnDefinition colDef) {
		this.colDef = colDef;
	}

	/*
	public Column getColumnModified() {
		return columnModified;
	}

	public void setColumnModified(Column columnModified) {
		this.columnModified = columnModified;
	}

	public ColDataType getNewDataType() {
		return newDataType;
	}

	public void setNewDataType(ColDataType newDataType) {
		this.newDataType = newDataType;
	}

	public List getOptions() {
		return options;
	}

	public void setOptions(List options) {
		this.options = options;
	}
	*/
	
	@Override
	public String toString(){
		String stmt = "ALTER TABLE MODIFY COLUMN" + this.colDef.getColumnName().getColumnName() + this.colDef.getColDataType().getDataTypeName();
		stmt += this.getColumnPositionName()!=null?this.getColumnPosition() + this.getColumnPositionName().getColumnName():"";
		return stmt;
	}
	
}
