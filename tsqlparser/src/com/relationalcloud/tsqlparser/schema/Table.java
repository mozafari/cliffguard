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

import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.parser.ParseException;
import com.relationalcloud.tsqlparser.schema.datatypes.TimestampDataType;
import com.relationalcloud.tsqlparser.statement.select.FromItem;
import com.relationalcloud.tsqlparser.statement.select.FromItemVisitor;
import com.relationalcloud.tsqlparser.statement.select.IntoTableVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A table. It can have an alias and the schema name it belongs to. 
 */
public class Table implements FromItem {
	private String schemaName;

	private boolean asOf = false;
	private boolean between = false;
	private boolean after = false;
	private boolean before = false;
	private boolean symmetric = true;
	private boolean temporal = false;
	
	public void setTemporal(boolean t){
		temporal=t;		
	}
	
	public boolean isTemporal(){
		return temporal;
	}
	
	public void setSymmetric(boolean t){
		symmetric=t;		
	}
	
	public boolean isAsOf() {
		
		return asOf;
	}

	public void setAsOf(boolean asOf) throws ParseException {
		if(after || between || before)
			throw new ParseException("Multiple temporal predicates for the same table!");
		this.asOf = asOf;
	}

	public boolean isBetween() {
		return between;
	}

	public void setBetween(boolean between) throws ParseException {
		if(after || asOf || before)
			throw new ParseException("Multiple temporal predicates for the same table!");
		this.between = between;
	}

	public boolean isAfter() {
		return after;
	}

	public void setAfter(boolean after) throws ParseException {
		if(asOf || between || before)
			throw new ParseException("Multiple temporal predicates for the same table!");
		this.after = after;
	}

	public boolean isBefore() {
		return before;
	}

	public void setBefore(boolean before) throws ParseException {
		if(after || between || asOf)
			throw new ParseException("Multiple temporal predicates for the same table!");
		this.before = before;
	}

	public TimestampDataType getTimestamp2() {
		return timestamp2;
	}

	public TimestampDataType getTimestamp1() {
		return timestamp1;
	}

	private String name;
	private String alias;
	private TimestampDataType timestamp1;
	private TimestampDataType timestamp2;
	
	public Table() {
	}

	public Table(String schemaName, String name) {
		this.schemaName = schemaName;
		this.name = name;
	}
	
	public Table(SchemaTable schemaTable) {
		name = schemaTable.getTableName();
	}

	public String getName() {
		return name;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setName(String string) {
		name = string;
	}

	public void setSchemaName(String string) {
		schemaName = string;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String string) {
		alias = string;
	}
	
	public String getWholeTableName() {

		String tableWholeName = null;
		if (name == null) {
			return null;
		}
		if (schemaName != null) {
			tableWholeName = schemaName + "." + name;
		} else {
			tableWholeName = name;
		}
		
		return tableWholeName;

	}

	public void accept(FromItemVisitor fromItemVisitor) {
		fromItemVisitor.visit(this);
	}
	
	public void accept(IntoTableVisitor intoTableVisitor) {
		intoTableVisitor.visit(this);
	}
	
	
	public String toString() {
		return getWholeTableName()+((alias!=null)?" AS "+alias:"");
	}
	
	public boolean equals(Table test){
		
		return schemaName.equalsIgnoreCase(test.schemaName) && name.equals(test.name) && alias.equals(test.alias);
		
	}

	public void setTimestamp1(TimestampDataType timestamp1) {
		this.timestamp1=timestamp1;
	}
	
	public void setTimestamp2(TimestampDataType timestamp2) {
		this.timestamp2=timestamp2;
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
