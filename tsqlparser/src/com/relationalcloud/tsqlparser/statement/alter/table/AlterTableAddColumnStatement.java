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

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;


/**
 * An Alter ADD COLUMN alter table statement
 * Syntax:
 * <ul>
 * 	<li>ADD [COLUMN] col_name column_definition [FIRST | AFTER col_name ]</li>
 * 	<li>ADD [COLUMN] (col_name column_definition,...)</li>
 * </ul>
 * @author fangar
 *
 */
public class AlterTableAddColumnStatement  extends AlterTableStatement{

	private ArrayList<ColumnDefinition> columnDefinitions;
	private String position;
	private Column columnPosition;
	
	
	
	public ArrayList<ColumnDefinition> getColumnDefinitions() {
		return columnDefinitions;
	}

	public void setColumnDefinitions(ArrayList<ColumnDefinition> columnDefinitions) {
		this.columnDefinitions = columnDefinitions;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public Column getColumnPosition() {
		return columnPosition;
	}

	public void setColumnPosition(Column columnPosition) {
		this.columnPosition = columnPosition;
	}
	
	
	
}
