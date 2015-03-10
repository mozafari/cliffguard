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

package com.relationalcloud.tsqlparser.statement.update;

import java.util.List;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * The update statement.
 */
public class Update implements Statement {
	// old replaced by a list of tables
	//private Table table;
	private List tables;
	private Expression where;
	private List columns;
	private List expressions;

	public void accept(StatementVisitor statementVisitor) {
		statementVisitor.visit(this);
	}

	/*public Table getTable() {
		return table;
	}*/

	public Expression getWhere() {
		return where;
	}

	/*public void setTable(Table name) {
		table = name;
	}*/

	public void setWhere(Expression expression) {
		where = expression;
	}

	/**
	 * The {@link com.relationalcloud.tsqlparser.schema.Column}s in this update (as col1 and col2 in UPDATE col1='a', col2='b')
	 * @return a list of {@link com.relationalcloud.tsqlparser.schema.Column}s
	 */
	public List getColumns() {
		return columns;
	}

	/**
	 * The {@link Expression}s in this update (as 'a' and 'b' in UPDATE col1='a', col2='b')
	 * @return a list of {@link Expression}s
	 */
	public List getExpressions() {
		return expressions;
	}

	public void setColumns(List list) {
		columns = list;
	}

	public void setExpressions(List list) {
		expressions = list;
	}

	public List getTables() {
		return tables;
	}

	public void setTables(List tables) {
		this.tables = tables;
	}

	@Override
	public void accept(RecursiveVisitor recursiveVisitor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object accept(RecursiveRewriterVisitor recursiveRewriterVisitor) {
		// TODO Auto-generated method stub
		return null;
	}

}
