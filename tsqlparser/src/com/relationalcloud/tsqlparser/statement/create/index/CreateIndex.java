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

package com.relationalcloud.tsqlparser.statement.create.index;

import java.util.List;

import com.relationalcloud.tsqlparser.schema.Index;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;



/**
 * A CREATE INDEX statement
 * <br>
 * Syntax:
 * <br>
 * <br>CREATE [ONLINE|OFFLINE] [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name [index_type] 
 * <br>ON tbl_name (index_col_name,...) [index_option] ...
 * <br>index_col_name:
 * <br>col_name [(length)] [ASC | DESC]
 * <br>index_type: USING {BTREE | HASH | RTREE}
 * <br>index_option: 
 * <br>KEY_BLOCK_SIZE [=] value | 
 * <br>index_type |
 * <br>WITH PARSER parser_name
 * 
 * @author fangar
 *
 */
public class CreateIndex implements Statement {

	//ONLINE, OFFLINE options
	private List createOptions;
	private Table table;
	private Index index;
	
	
	public List getCreateOptions() {
		return createOptions;
	}
	public void setCreateOptions(List createOptions) {
		this.createOptions = createOptions;
	}
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
	}
	
	public Index getIndex() {
		return index;
	}
	public void setIndex(Index index) {
		this.index = index;
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
