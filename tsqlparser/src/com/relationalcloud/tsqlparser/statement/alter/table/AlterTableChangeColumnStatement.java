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
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;


/**
 * An CHANGE COLUMN statement
 * Syntax:
 * CHANGE [COLUMN] old_col_name new_col_name column_definition [FIRST|AFTER col_name]
 * @author fangar
 *
 */
public class AlterTableChangeColumnStatement extends AlterTableStatement implements Statement{

	private Column columnOld;
	private ColumnDefinition newColumnDefinition;
	private String columnPosition;
	private Column columnPositionName;

	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		
	}

	public Column getColumnOld() {
		return columnOld;
	}

	public void setColumnOld(Column columnOld) {
		this.columnOld = columnOld;
	}

	public ColumnDefinition getNewColumnDefinition() {
		return newColumnDefinition;
	}

	public void setNewColumnDefinition(ColumnDefinition newColumnDefintion) {
		this.newColumnDefinition = newColumnDefintion;
	}

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
	
}
