/**
 * 
 */
package com.relationalcloud.tsqlparser.visitors;

import com.relationalcloud.tsqlparser.exception.MatchingException;
import com.relationalcloud.tsqlparser.expression.AllComparisonExpression;
import com.relationalcloud.tsqlparser.expression.AnyComparisonExpression;
import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.CaseExpression;
import com.relationalcloud.tsqlparser.expression.DateValue;
import com.relationalcloud.tsqlparser.expression.DoubleValue;
import com.relationalcloud.tsqlparser.expression.ExpressionVisitor;
import com.relationalcloud.tsqlparser.expression.Function;
import com.relationalcloud.tsqlparser.expression.InverseExpression;
import com.relationalcloud.tsqlparser.expression.JdbcParameter;
import com.relationalcloud.tsqlparser.expression.LongValue;
import com.relationalcloud.tsqlparser.expression.NullValue;
import com.relationalcloud.tsqlparser.expression.Parenthesis;
import com.relationalcloud.tsqlparser.expression.StringValue;
import com.relationalcloud.tsqlparser.expression.TimeValue;
import com.relationalcloud.tsqlparser.expression.TimestampValue;
import com.relationalcloud.tsqlparser.expression.UnaryValue;
import com.relationalcloud.tsqlparser.expression.WhenClause;
import com.relationalcloud.tsqlparser.expression.operators.arithmetic.Addition;
import com.relationalcloud.tsqlparser.expression.operators.arithmetic.Division;
import com.relationalcloud.tsqlparser.expression.operators.arithmetic.Multiplication;
import com.relationalcloud.tsqlparser.expression.operators.arithmetic.Subtraction;
import com.relationalcloud.tsqlparser.expression.operators.conditional.AndExpression;
import com.relationalcloud.tsqlparser.expression.operators.conditional.OrExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.Between;
import com.relationalcloud.tsqlparser.expression.operators.relational.Concatenation;
import com.relationalcloud.tsqlparser.expression.operators.relational.EqualsTo;
import com.relationalcloud.tsqlparser.expression.operators.relational.ExistsExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.InExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.IsNullExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.LikeExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.NotEqualsTo;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;



/**
 * @author krl
 * 
 */
public class ExpressionIsInRangeVisitor implements ExpressionVisitor {

  int type = -1;
  boolean matches = false;

  BinaryExpression lrule;
  BinaryExpression ltest;

  public boolean isInRange(BinaryExpression test, BinaryExpression rule)
      throws MatchingException {

    // verify that at least one side of each BinaryExpression is a column
    if (!(test.getLeftExpression() instanceof Column)
        && !(test.getRightExpression() instanceof Column))
      throw new MatchingException("No columns in BinaryExpression");
    if (!(rule.getLeftExpression() instanceof Column)
        && !(rule.getRightExpression() instanceof Column))
      throw new MatchingException("No columns in BinaryExpression");

    // verify that only one side is a column (no join should get here)
    if ((test.getLeftExpression() instanceof Column)
        && (test.getRightExpression() instanceof Column))
      throw new MatchingException("Both columns in BinaryExpression");
    if ((rule.getLeftExpression() instanceof Column)
        && (rule.getRightExpression() instanceof Column))
      throw new MatchingException("Both columns in BinaryExpression");

    // reorder expression to keep columns on left
    ltest = invertIfNeeded(test);
    lrule = invertIfNeeded(rule);

    lrule.accept(this);
    return matches;

  }

  private BinaryExpression invertIfNeeded(BinaryExpression input)
      throws MatchingException {

    BinaryExpression output = null;
    // reorder expression so to have columns on the left
    if (input.getRightExpression() instanceof Column) {

      // determin operator
      if (input instanceof GreaterThan)
        output = new MinorThanEquals();
      if (input instanceof GreaterThanEquals)
        output = new MinorThan();
      if (input instanceof MinorThan)
        output = new GreaterThanEquals();
      if (input instanceof MinorThanEquals)
        output = new GreaterThan();
      if (input instanceof EqualsTo)
        output = new EqualsTo();

      if (output == null)
        throw new MatchingException("Operator not supported: "
            + input.toString());

      // invert columns
      output.setLeftExpression(input.getRightExpression());
      output.setRightExpression(input.getLeftExpression());

    } else {
      output = input;
    }
    return output;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.NullValue)
   */
  @Override
  public void visit(NullValue nullValue) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.Function)
   */
  @Override
  public void visit(Function function) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.InverseExpression)
   */
  @Override
  public void visit(InverseExpression inverseExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.JdbcParameter)
   */
  @Override
  public void visit(JdbcParameter jdbcParameter) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.DoubleValue)
   */
  @Override
  public void visit(DoubleValue doubleValue) {

    DoubleValue testval = (DoubleValue) ltest.getRightExpression();
    DoubleValue ruleval = doubleValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  private boolean comparValues(UnaryValue testval, UnaryValue ruleval) {

    boolean m = false;

    if (lrule instanceof EqualsTo) {
      if (ltest instanceof EqualsTo)
        if (ruleval.compareTo(testval) == 0)
          m = true;
      if (ltest instanceof MinorThan)
        if (ruleval.compareTo(testval) > 0)
          m = true;
      if (ltest instanceof MinorThanEquals)
        if (ruleval.compareTo(testval) >= 0)
          m = true;
      if (ltest instanceof GreaterThan)
        if (ruleval.compareTo(testval) < 0)
          m = true;
      if (ltest instanceof GreaterThanEquals)
        if (ruleval.compareTo(testval) <= 0)
          m = true;
    }

    if (lrule instanceof MinorThan) {
      if (ltest instanceof EqualsTo)
        if (ruleval.compareTo(testval) > 0)
          m = true;
      if (ltest instanceof MinorThan)
        m = true;
      if (ltest instanceof MinorThanEquals)
        m = true;
      if (ltest instanceof GreaterThan)
        if (ruleval.compareTo(testval) > 0)
          m = true;
      if (ltest instanceof GreaterThanEquals)
        if (ruleval.compareTo(testval) > 0)
          m = true;
    }

    if (lrule instanceof MinorThanEquals) {
      if (ltest instanceof EqualsTo)
        if (ruleval.compareTo(testval) >= 0)
          m = true;
      if (ltest instanceof MinorThan)
        m = true;
      if (ltest instanceof MinorThanEquals)
        m = true;
      if (ltest instanceof GreaterThan)
        if (ruleval.compareTo(testval) > 0)
          m = true;
      if (ltest instanceof GreaterThanEquals)
        if (ruleval.compareTo(testval) >= 0)
          m = true;
    }

    if (lrule instanceof GreaterThan) {
      if (ltest instanceof EqualsTo)
        if (ruleval.compareTo(testval) < 0)
          m = true;
      if (ltest instanceof MinorThan)
        if (ruleval.compareTo(testval) < 0)
          m = true;
      if (ltest instanceof MinorThanEquals)
        if (ruleval.compareTo(testval) < 0)
          m = true;
      if (ltest instanceof GreaterThan)
        m = true;
      if (ltest instanceof GreaterThanEquals)
        m = true;
    }

    if (lrule instanceof GreaterThanEquals) {
      if (ltest instanceof EqualsTo)
        if (ruleval.compareTo(testval) <= 0)
          m = true;
      if (ltest instanceof MinorThan)
        if (ruleval.compareTo(testval) < 0)
          m = true;
      if (ltest instanceof MinorThanEquals)
        if (ruleval.compareTo(testval) <= 0)
          m = true;
      if (ltest instanceof GreaterThan)
        m = true;
      if (ltest instanceof GreaterThanEquals)
        m = true;
    }

    return m;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.LongValue)
   */
  @Override
  public void visit(LongValue longValue) {
    LongValue testval = (LongValue) ltest.getRightExpression();
    LongValue ruleval = longValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.DateValue)
   */
  @Override
  public void visit(DateValue dateValue) {
    DateValue testval = (DateValue) ltest.getRightExpression();
    DateValue ruleval = dateValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.TimeValue)
   */
  @Override
  public void visit(TimeValue timeValue) {
    TimeValue testval = (TimeValue) ltest.getRightExpression();
    TimeValue ruleval = timeValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.TimestampValue)
   */
  @Override
  public void visit(TimestampValue timestampValue) {
    TimestampValue testval = (TimestampValue) ltest.getRightExpression();
    TimestampValue ruleval = timestampValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.Parenthesis)
   */
  @Override
  public void visit(Parenthesis parenthesis) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.StringValue)
   */
  @Override
  public void visit(StringValue stringValue) {
    StringValue testval = (StringValue) ltest.getRightExpression();
    StringValue ruleval = stringValue;

    matches = comparValues(new UnaryValue(testval), new UnaryValue(ruleval));

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.arithmetic.Addition)
   */
  @Override
  public void visit(Addition addition) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.arithmetic.Division)
   */
  @Override
  public void visit(Division division) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.arithmetic.Multiplication)
   */
  @Override
  public void visit(Multiplication multiplication) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.arithmetic.Subtraction)
   */
  @Override
  public void visit(Subtraction subtraction) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.conditional.AndExpression)
   */
  @Override
  public void visit(AndExpression andExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.conditional.OrExpression)
   */
  @Override
  public void visit(OrExpression orExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.Between)
   */
  @Override
  public void visit(Between between) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.EqualsTo)
   */
  @Override
  public void visit(EqualsTo equalsTo) {
    equalsTo.getRightExpression().accept(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.GreaterThan)
   */
  @Override
  public void visit(GreaterThan greaterThan) {
    greaterThan.getRightExpression().accept(this);

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.GreaterThanEquals)
   */
  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    greaterThanEquals.getRightExpression().accept(this);

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.InExpression)
   */
  @Override
  public void visit(InExpression inExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.IsNullExpression)
   */
  @Override
  public void visit(IsNullExpression isNullExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.LikeExpression)
   */
  @Override
  public void visit(LikeExpression likeExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.MinorThan)
   */
  @Override
  public void visit(MinorThan minorThan) {
    minorThan.getRightExpression().accept(this);

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.MinorThanEquals)
   */
  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    minorThanEquals.getRightExpression().accept(this);

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.NotEqualsTo)
   */
  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * schema.Column)
   */
  @Override
  public void visit(Column tableColumn) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * statement.select.SubSelect)
   */
  @Override
  public void visit(SubSelect subSelect) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.CaseExpression)
   */
  @Override
  public void visit(CaseExpression caseExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.WhenClause)
   */
  @Override
  public void visit(WhenClause whenClause) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.operators.relational.ExistsExpression)
   */
  @Override
  public void visit(ExistsExpression existsExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.AllComparisonExpression)
   */
  @Override
  public void visit(AllComparisonExpression allComparisonExpression) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.
   * expression.AnyComparisonExpression)
   */
  @Override
  public void visit(AnyComparisonExpression anyComparisonExpression) {
    // TODO Auto-generated method stub

  }

@Override
public void visit(Concatenation concatenationExpression) {
	// TODO Auto-generated method stub
	
}

}
