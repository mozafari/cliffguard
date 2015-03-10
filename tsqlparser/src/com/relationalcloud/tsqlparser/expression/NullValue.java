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
 *  A "NULL" in a sql statement
 */
public class NullValue implements ScalarExpression {
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		v.visitEnd(this);
	}
	
	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		return v.visitEnd(this);
	}
	
	public String toString() {
		return "NULL";
	}

	@Override
	public ScalarExpression copy() {
		return this; // no member fields
	}

	@Override
	public Object literalValue() {
		return null;
	}
	
	@Override
	public ScalarExpression constructFrom(Object o) {
		if (o != null)
			throw new IllegalArgumentException("null value cannot be constructed from non-null value");
		return this;
	}
}
