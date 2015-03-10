/**
 * 
 */
package com.relationalcloud.tsqlparser.visitors;

import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsList;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsListVisitor;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;


/**
 * Implements an extractor for VALUE only
 * 
 * @author krl
 * 
 */
public class ValuesListVisitor implements ItemsListVisitor {

  ExpressionList explist;

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor#visit
   * (net.sf.jsqlparser.statement.select.SubSelect)
   */
  @Override
  public void visit(SubSelect subSelect) {
    // TODO unimplemented

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor#visit
   * (net.sf.jsqlparser.expression.operators.relational.ExpressionList)
   */
  @Override
  public void visit(ExpressionList expressionList) {
    explist = expressionList;

  }

  public java.util.List getListValue(ItemsList v) {

    v.accept(this);
    return explist.getExpressions();
  }

}
