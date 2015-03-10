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

import java.util.List;

import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * CASE/WHEN expression.
 * 
 * Syntax:
 * <code><pre>
 * CASE 
 * WHEN condition THEN expression
 * [WHEN condition THEN expression]...
 * [ELSE expression]
 * END
 * </pre></code>
 * 
 * <br/>
 * or <br/>
 * <br/>
 * 
 * <code><pre>
 * CASE expression 
 * WHEN condition THEN expression
 * [WHEN condition THEN expression]...
 * [ELSE expression]
 * END
 * </pre></code>
 *  
 *  See also:
 *  https://aurora.vcu.edu/db2help/db2s0/frame3.htm#casexp
 *  http://sybooks.sybase.com/onlinebooks/group-as/asg1251e/commands/@ebt-link;pt=5954?target=%25N%15_52628_START_RESTART_N%25
 *  
 *  
 * @author Havard Rast Blok
 */
public class CaseExpression implements Expression {

	private Expression switchExpression;
	
	private List whenClauses;
	
	private Expression elseExpression;
	
	/* (non-Javadoc)
	 * @see net.sf.jsqlparser.expression.Expression#accept(net.sf.jsqlparser.expression.ExpressionVisitor)
	 */
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}
	
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		if (getSwitchExpression() != null)
			getSwitchExpression().accept(v);
		List<WhenClause> whenClauses = getWhenClauses();
		for (WhenClause wc : whenClauses)
			wc.accept(v);
		if (getElseExpression() != null)
			getElseExpression().accept(v);
		v.visitEnd(this);
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		Object s = switchExpression != null ? switchExpression.accept(v) : null;
		if (s != null)
			switchExpression = (Expression) s;
		List<WhenClause> whenClauses = getWhenClauses();
		for (int i = 0; i < whenClauses.size(); i++) {
			Object w = whenClauses.get(i).accept(v);
			if (w != null)
				whenClauses.set(i, (WhenClause) w);
		}
		Object e = elseExpression != null ? elseExpression.accept(v) : null;
		if (e != null)
			elseExpression = (Expression) e;
		return v.visitEnd(this);
	}
	
	/**
	 * @return Returns the switchExpression.
	 */
	public Expression getSwitchExpression() {
		return switchExpression;
	}
	/**
	 * @param switchExpression The switchExpression to set.
	 */
	public void setSwitchExpression(Expression switchExpression) {
		this.switchExpression = switchExpression;
	}
	
	/**
	 * @return Returns the elseExpression.
	 */
	public Expression getElseExpression() {
		return elseExpression;
	}
	/**
	 * @param elseExpression The elseExpression to set.
	 */
	public void setElseExpression(Expression elseExpression) {
		this.elseExpression = elseExpression;
	}
	/**
	 * @return Returns the whenClauses.
	 */
	public List getWhenClauses() {
		return whenClauses;
	}
	
	/**
	 * @param whenClauses The whenClauses to set.
	 */
	public void setWhenClauses(List whenClauses) {
		this.whenClauses = whenClauses;
	}
	
	public String toString() {
		return "CASE "+((switchExpression!=null)?switchExpression+" ":"")+
				PlainSelect.getStringList(whenClauses,false, false)+" "+
				((elseExpression!=null)?"ELSE "+elseExpression+" ":"")+
				"END";
	}

}
