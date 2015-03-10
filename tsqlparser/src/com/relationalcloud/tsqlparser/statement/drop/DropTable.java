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

package com.relationalcloud.tsqlparser.statement.drop;

import java.util.List;

import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A DROP TABLE statement
 * <br>Syntax:
 * <br>DROP [TEMPORARY] TABLE [IF EXISTS] tbl_name [, tbl_name] ... [RESTRICT | CASCADE]
 * @author fangar
 *
 */
public class DropTable implements Drop {

	
	private List<Table> tables;
	private String option;
	
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		
	}

	public List<Table> getTable() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

	public void setTable(List<Table> table) {
		this.tables = table;
	}
	
	public String toString(){
		String sql="";
		
		sql+= "DROP TABLE ";
		//at least one table in the list
		
		for(int i=0;i<getTable().size();i++){
			sql += getTable().get(i) + ((i<getTable().size()-1)?",":""); 
		}
		sql+= getOption()!=null?getOption():"";
		return sql;
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
