package com.relationalcloud.tsqlparser.expression;

public interface ScalarExpression extends Expression {
	public ScalarExpression copy();
	public Object literalValue();
	public ScalarExpression constructFrom(Object o);
}
