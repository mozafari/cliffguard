package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.expression.AllComparisonExpression;
import com.relationalcloud.tsqlparser.expression.AnyComparisonExpression;
import com.relationalcloud.tsqlparser.expression.CaseExpression;
import com.relationalcloud.tsqlparser.expression.DateValue;
import com.relationalcloud.tsqlparser.expression.DoubleValue;
import com.relationalcloud.tsqlparser.expression.Expression;
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
import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.InExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.IsNullExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsListVisitor;
import com.relationalcloud.tsqlparser.expression.operators.relational.LikeExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.NotEqualsTo;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.drop.DropTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.replace.Replace;
import com.relationalcloud.tsqlparser.statement.select.AllColumns;
import com.relationalcloud.tsqlparser.statement.select.AllTableColumns;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.select.SelectExpressionItem;
import com.relationalcloud.tsqlparser.statement.select.SelectItemVisitor;
import com.relationalcloud.tsqlparser.statement.select.SelectVisitor;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;
import com.relationalcloud.tsqlparser.statement.select.Union;
import com.relationalcloud.tsqlparser.statement.truncate.Truncate;
import com.relationalcloud.tsqlparser.statement.update.Update;


/**
 * Extract the {@link Column} for the current statement
 * 
 * @author fangar
 * 
 */
public class ExtractColumnsVisitor implements StatementVisitor, SelectVisitor,
    ItemsListVisitor, SelectItemVisitor, ExpressionVisitor {

  private ArrayList<Column> selectedColumns;
  private Expression whereCondition;

  public void visit(SubSelect subSelect) {
    subSelect.getSelectBody().accept(this);
  }

  public ArrayList<Column> getSelectedColumns(Insert insert) {
    selectedColumns = new ArrayList<Column>();
    insert.getItemsList().accept(this);
    return selectedColumns;
  }

  public Expression getWhereCondition(Insert insert) {
    insert.getItemsList().accept(this);
    return whereCondition;
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    for (SelectExpressionItem item : (ArrayList<SelectExpressionItem>) plainSelect
        .getSelectItems()) {
      item.getExpression().accept(this);
    }
    if (plainSelect.getWhere() != null) {
      whereCondition = plainSelect.getWhere();
    }
  }

  public void visit(Column column) {
    selectedColumns.add(column);
  }

  @Override
  public void visit(Insert insert) {
    insert.getItemsList().accept(this);
  }

  @Override
  public void visit(Delete arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(CreateTable arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AlterTableStatement arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Replace arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Select arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Update arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Truncate arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Union arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(ExpressionList arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AllColumns arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AllTableColumns arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(SelectExpressionItem arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(NullValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Function arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(InverseExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(JdbcParameter arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(DoubleValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(LongValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(DateValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(TimeValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(TimestampValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Parenthesis arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(StringValue arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Addition arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Division arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Multiplication arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Subtraction arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AndExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(OrExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Between arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(EqualsTo arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(GreaterThan arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(GreaterThanEquals arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(InExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(IsNullExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(LikeExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(MinorThan arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(MinorThanEquals arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(NotEqualsTo arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(CaseExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(WhenClause arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(ExistsExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AllComparisonExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AnyComparisonExpression arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(DropTable drop) {
    // TODO Auto-generated method stub

  }

@Override
public void visit(Concatenation concatenationExpression) {
	// TODO Auto-generated method stub
	
}

}
