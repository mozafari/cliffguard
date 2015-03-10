package com.relationalcloud.tsqlparser.expression;

public class UnaryValue {

  Expression e;

  public UnaryValue(Expression e) {
    this.e = e;

  }

  public int compareTo(UnaryValue testval) {

    Expression f = testval.e;

    if (e instanceof DoubleValue && f instanceof DoubleValue)
      return Double.valueOf(((DoubleValue) e).getValue()).compareTo(
          Double.valueOf(((DoubleValue) f).getValue()));

    if (e instanceof DateValue && f instanceof DateValue)
      return ((DateValue) e).getValue().compareTo(((DateValue) f).getValue());

    if (e instanceof TimeValue && f instanceof TimeValue)
      return ((TimeValue) e).getValue().compareTo(((TimeValue) f).getValue());

    if (e instanceof TimestampValue && f instanceof TimestampValue)
      return ((TimestampValue) e).getValue().compareTo(
          ((TimestampValue) f).getValue());

    if (e instanceof StringValue && f instanceof StringValue)
      return ((StringValue) e).getValue().compareTo(
          ((StringValue) f).getValue());

    if (e instanceof LongValue && f instanceof LongValue)
      return Long.valueOf(((LongValue) e).getValue()).compareTo(
          Long.valueOf(((LongValue) f).getValue()));

    throw new RuntimeException("Mismatching Types");

  }

}
