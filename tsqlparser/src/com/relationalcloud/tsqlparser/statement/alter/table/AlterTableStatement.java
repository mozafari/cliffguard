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

import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * An ALTER TABLE statement.
 * For parsing simplicity, the statement has been divided into different ALTER TABLE sub-statements:
 * <ul>
 * <li>ADD COLUMN</li>
 * <li>ADD CONSTRAINT</li>
 * <li>ALTER COLUMN</li>
 * <li>CHANGE COLUMN</li>
 * <li>DROP COLUMN</li>
 * <li>DROP CONSTRAINT</li>
 * <li>MODIFY COLUMN</li>
 * <li>ORDER BY</li>
 * <li>RENAME TABLE</li>
 * </ul>
 * Each statement will be explained in its class
 *
 * @author fangar
 *
 */
public class AlterTableStatement implements Statement {

	private Table table;
	private String tableOptions;
	
	public void accept(StatementVisitor alterTableVisitor) {
		alterTableVisitor.visit(this);
	}
	
	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public String getTableOptions() {
		return tableOptions;
	}

	public void setTableOptions(String tableOptions) {
		this.tableOptions = tableOptions;
	}

	@Override
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		v.visitEnd(this);
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		return v.visitEnd(this);
	}
	
}
