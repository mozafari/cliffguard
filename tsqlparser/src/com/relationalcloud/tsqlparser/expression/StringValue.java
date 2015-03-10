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
 
package com.relationalcloud.tsqlparser.expression;

import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;

/**
 * A string as in 'example_string'
 */
public class StringValue implements ScalarExpression {
	private String value = "";
	
	public StringValue(String escapedValue) {
		// removing "'" at the start and at the end 
	//	if(escapedValue.length()>0)
			value = escapedValue.substring(1, escapedValue.length() - 1);
	}
	
	public StringValue(StringValue value) {
		this.value = value.value;
	}
	
	public String getValue() {
		return value;
	}

	public String getNotExcapedValue() {
		StringBuffer buffer = new StringBuffer(value);
		int index = 0;
		int deletesNum = 0;
		while ((index = value.indexOf("''", index)) != -1) {
			buffer.deleteCharAt(index-deletesNum);
			index+=2;
			deletesNum++;
		}
		return buffer.toString();
	}

	public void setValue(String string) {
		value = string;
	}
	
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	public String toString() {
		return "'"+value+"'";
	}

	@Override
	public void accept(RecursiveVisitor recursiveVisitor) {
		recursiveVisitor.visitBegin(this);
		recursiveVisitor.visitEnd(this);
	}
	
	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		return v.visitEnd(this);
	}

	@Override
	public ScalarExpression copy() {
		return new StringValue(this);
	}

	@Override
	public Object literalValue() {
		return value;
	}
	
	@Override
	public ScalarExpression constructFrom(Object o) {
		String s = (String) o;
		return new StringValue("'" + s + "'");
	}
}
