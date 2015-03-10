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
 
 package com.relationalcloud.tsqlparser.expression.operators.relational;

import java.util.List;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A list of expressions, as in SELECT A FROM TAB WHERE B IN (expr1,expr2,expr3)
 */
public class ExpressionList implements ItemsList {
	private List expressions;

	public ExpressionList() {
	}

	public ExpressionList(List expressions) {
		this.expressions = expressions;
	}

	public List getExpressions() {
		return expressions;
	}

	public void setExpressions(List list) {
		expressions = list;
	}

	public void accept(ItemsListVisitor itemsListVisitor) {
		itemsListVisitor.visit(this);
	}
	
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		List<Expression> exprs = getExpressions();
		for (Expression e : exprs)
			e.accept(v);
		v.visitEnd(this);
	}
	
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		List<Expression> exprs = getExpressions();
		for (int i = 0; i < exprs.size(); i++) {
			Object e = exprs.get(i).accept(v);
			if (e != null)
				exprs.set(i, (Expression) e);
		}
		return v.visitEnd(this);
	}

	public String toString() {
		return PlainSelect.getStringList(expressions, true, true);
	}
}
