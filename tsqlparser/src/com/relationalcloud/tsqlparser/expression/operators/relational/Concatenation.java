package com.relationalcloud.tsqlparser.expression.operators.relational;

import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.ExpressionVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


public class Concatenation extends BinaryExpression {

	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}


	public String getStringExpression() {
		return "||";
	}
	

	public String toString() {
	    String retval = super.toString();
	    
	    return retval;
	}
    
	@Override
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		getLeftExpression().accept(v);
		getRightExpression().accept(v);
		v.visitEnd(this);
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		Object l = getLeftExpression().accept(v);
		if (l != null)
			setLeftExpression((Expression) l);
		Object r = getRightExpression().accept(v);
		if (r != null)
			setRightExpression((Expression) r);
		return v.visitEnd(this);
	}
}
