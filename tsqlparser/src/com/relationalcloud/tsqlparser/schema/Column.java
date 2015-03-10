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


package com.relationalcloud.tsqlparser.schema;

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.ExpressionVisitor;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.select.ColumnReference;
import com.relationalcloud.tsqlparser.statement.select.ColumnReferenceVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A column. It can have the table name it belongs to. 
 */
public class Column implements Expression, ColumnReference {

	private String columnName = "";
	private Table table;
	
	public Column(Table table, String columnName) {
		this.table = table;
		this.columnName=columnName;
	}
	
	// copy constructor
	public Column(Column c) {
		this.table = c.table;
		this.columnName = c.columnName;
	}
	
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		v.visitEnd(this);
	}
	
	public String getColumnName() {
		return columnName;
	}

	public Table getTable() {
		return table;
	}

	/**
	 * Table for columsn with null Table... reading from schema... 
	 * @param schema
	 * @return
	 * @throws Exception 
	 */
	public Table getTable(ArrayList<String> candidates, Schema schema) throws Exception {
		
		if(table!=null && table.getName()!=null &&  candidates!=null && candidates.contains(table.getName().replaceAll("`","")))
			return table;
		
		ArrayList<SchemaTable> st = schema.getTableByColumn(columnName);
		ArrayList<SchemaTable> st2 = new ArrayList<SchemaTable>(); 
		
		//focus only on the candidates
		for(SchemaTable s:st){
			for(String tabname:candidates)
			if(s.getTableName().replaceAll("`","").equals(tabname.replaceAll("`","")))
				st2.add(s);
		}
		
		if(st2.size()==1)
			return new Table(st2.get(0));
					
		return null;			
//		else{
//			String out = "";
//			String out2 = "";
//			for(SchemaTable ss:st)
//				out+= ", " + ss.getTableName();		
//			
//			for(SchemaTable ss:st2)
//				out2+= ", " + ss.getTableName();				
//			
//			throw new Exception("Column " +columnName +" is not present or ambiguous in this schema... st:" + out + " st2:" + out2);
//		}
//		return table;
//		
	}

	
	
	public void setColumnName(String string) {
		columnName = string;
	}

	public void setTable(Table table) {
		this.table = table;
	}
	
	/**
	 * @return the name of the column, prefixed with 'tableName' and '.' 
	 */
	public String getWholeColumnName() {
		
		String columnWholeName = null;
		String tableWholeName = "";
		  
		//CARLO: fixed using alias when present  
		if(table!=null)
		{
			if(table.getAlias()!=null && table.getAlias()!= "")
			{
			  tableWholeName = table.getAlias();
			}
			else
			{
			  tableWholeName = table.getWholeTableName();
		    }
			
			if (tableWholeName != null && tableWholeName.length() != 0) 
			{
				if(!tableWholeName.startsWith("`"))
					//Djellel: Removed the "`"
					tableWholeName = tableWholeName;	
				columnWholeName = tableWholeName + "." + columnName;
			} 
			else 
			{
				columnWholeName = columnName;
			}
		}
		else 
			columnWholeName = columnName;
		return columnWholeName;
	}
	
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	public void accept(ColumnReferenceVisitor columnReferenceVisitor) {
		columnReferenceVisitor.visit(this);
	}


	public String toString() {
		return getWholeColumnName();
	}
	
	public boolean equals(Column test){
		
		return test.columnName.equalsIgnoreCase(columnName) && test.table.equals(table); 
		
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		return v.visitEnd(this);
	}


}
