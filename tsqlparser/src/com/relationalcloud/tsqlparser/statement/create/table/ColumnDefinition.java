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

package com.relationalcloud.tsqlparser.statement.create.table;

import java.util.List;

import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.datatypes.DataType;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;


/**
 * A column definition in a CREATE TABLE statement.<br>
 * Example: mycol VARCHAR(30) NOT NULL
 */
public class ColumnDefinition {
	//private String columnName;
	private Column columnName;
	private DataType colDataType;
	private List columnSpecStrings;
	
	/**
	 * A list of strings of every word after the datatype of the column.<br>
	 * Example ("NOT", "NULL")
	 */
	public List getColumnSpecStrings() {
		return columnSpecStrings;
	}

	public void setColumnSpecStrings(List list) {
		columnSpecStrings = list;
	}

	/**
	 * The {@link ColDataType} of this column definition 
	 */
	public DataType getColDataType() {
		return colDataType;
	}

	public void setColDataType(DataType type) {
		colDataType = type;
	}

	public Column getColumnName() {
		return columnName;
	}

	public void setColumnName(Column column) {
		columnName = column;
	}
	
	public String toString() {
		return columnName+" "+colDataType+" "+PlainSelect.getStringList(columnSpecStrings, false, false);
	}

}
