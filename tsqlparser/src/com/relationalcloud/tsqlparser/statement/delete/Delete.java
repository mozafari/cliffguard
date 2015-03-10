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
 
package com.relationalcloud.tsqlparser.statement.delete;

import java.util.List;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


public class Delete implements Statement {
	//REPLACED BY LIST OF TABLES
	//private Table table;
	private List<Table> tables;
	private Expression where;
	
	public void accept(StatementVisitor statementVisitor) {
		statementVisitor.visit(this);
	}

	public Expression getWhere() {
		return where;
	}

	public void setWhere(Expression expression) {
		where = expression;
	}

	public String toString() {
		String retValue = "DELETE FROM ";
		Table t = tables.get(0);
		retValue += t.getName();
		retValue += t.getAlias()!=null?"AS " + t.getAlias():"";
		if(tables.size()>1){
			for(Table table:tables){
				retValue+=",";
				retValue += table.getName();
				retValue += table.getAlias()!=null?"AS " + table.getAlias():"";
			}
		}
		retValue += ((where!=null)?" WHERE "+where:"");
		return retValue;
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
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
