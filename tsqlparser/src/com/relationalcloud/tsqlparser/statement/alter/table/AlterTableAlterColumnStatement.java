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


/**
 * An ALTER COLUMN statement
 * Syntax:
 * ALTER [COLUMN] col_name {SET DEFAULT literal | DROP DEFAULT}
 * @author fangar
 *
 */
public class AlterTableAlterColumnStatement extends AlterTableStatement implements Statement{

	private Column column;
	private boolean newDefault = false;
	private boolean dropDefault = false;
	private String defaultValue;
	
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
	}

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	public boolean isNewDefault() {
		return newDefault;
	}

	public void setNewDefault(boolean setNewDefault) {
		this.newDefault = setNewDefault;
	}

	public boolean isDropDefault() {
		return dropDefault;
	}

	public void setDropDefault(boolean dropDefault) {
		this.dropDefault = dropDefault;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	
	
	
}
