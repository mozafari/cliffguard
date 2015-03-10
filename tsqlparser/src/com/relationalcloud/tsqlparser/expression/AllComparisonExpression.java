package com.relationalcloud.tsqlparser.expression;

import com.relationalcloud.tsqlparser.statement.select.SubSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;

public class AllComparisonExpression implements Expression {
	private SubSelect subSelect;
	
	public AllComparisonExpression(SubSelect subSelect) {
		this.subSelect = subSelect;
	}
	
	public SubSelect GetSubSelect() {
		return subSelect;
	}
	
	public void accept(ExpressionVisitor expressionVisitor) {
		expressionVisitor.visit(this);
	}

	@Override
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		GetSubSelect().accept(v);
		v.visitEnd(this);
	}
	
	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		Object o = subSelect.accept(v);
		if (o != null)
			subSelect = (SubSelect) o;
		return v.visitEnd(this);
	}
	
}
