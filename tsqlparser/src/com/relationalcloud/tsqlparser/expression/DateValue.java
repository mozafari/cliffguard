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

import java.sql.Date;

import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A Date in the form {d 'yyyy-mm-dd'}
 */
public class DateValue implements ScalarExpression {
	private Date value;

	public DateValue(String value) {
		this.value = Date.valueOf(value.substring(1, value.length()-1));
	}
	
	public DateValue(Date value) {
		this.value = value;
	}
	
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		v.visitEnd(this);
	}

	public Date getValue() {
		return value;
	}

	public void setValue(Date d) {
		value = d;
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		return v.visitEnd(this);
	}

	@Override
	public ScalarExpression copy() {
		return new DateValue((Date) value.clone());
	}

	@Override
	public Object literalValue() {
		return value;
	}
	
	@Override
	public ScalarExpression constructFrom(Object o) {
		return new DateValue((Date) o);
	}
	
	@Override
	public String toString() {
		return "date '" + value.toString() + "'";
	}

}
