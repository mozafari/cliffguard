package com.relationalcloud.tsqlparser.visitors.recursive;

import com.relationalcloud.tsqlparser.expression.AllComparisonExpression;
import com.relationalcloud.tsqlparser.expression.AnyComparisonExpression;
import com.relationalcloud.tsqlparser.expression.CaseExpression;
import com.relationalcloud.tsqlparser.expression.DateValue;
import com.relationalcloud.tsqlparser.expression.DoubleValue;
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
import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.InExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.IsNullExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsList;
import com.relationalcloud.tsqlparser.expression.operators.relational.LikeExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.NotEqualsTo;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableAddColumnStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableAddConstraintStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableAlterColumnStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableChangeColumnStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableDropColumnStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableDropConstraintStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableModifyColumnStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableOrderByStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableRenameTableStatement;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.index.CreateIndex;
import com.relationalcloud.tsqlparser.statement.create.table.ColDataType;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.drop.Drop;
import com.relationalcloud.tsqlparser.statement.drop.DropIndex;
import com.relationalcloud.tsqlparser.statement.drop.DropTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.rename.RenameTable;
import com.relationalcloud.tsqlparser.statement.replace.Replace;
import com.relationalcloud.tsqlparser.statement.select.AllColumns;
import com.relationalcloud.tsqlparser.statement.select.AllTableColumns;
import com.relationalcloud.tsqlparser.statement.select.ColumnIndex;
import com.relationalcloud.tsqlparser.statement.select.Distinct;
import com.relationalcloud.tsqlparser.statement.select.FromItem;
import com.relationalcloud.tsqlparser.statement.select.Join;
import com.relationalcloud.tsqlparser.statement.select.Limit;
import com.relationalcloud.tsqlparser.statement.select.OrderByElement;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.select.SelectExpressionItem;
import com.relationalcloud.tsqlparser.statement.select.SelectItem;
import com.relationalcloud.tsqlparser.statement.select.SubJoin;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;
import com.relationalcloud.tsqlparser.statement.select.Top;
import com.relationalcloud.tsqlparser.statement.select.Union;
import com.relationalcloud.tsqlparser.statement.select.WithItem;
import com.relationalcloud.tsqlparser.statement.truncate.Truncate;
import com.relationalcloud.tsqlparser.statement.update.Update;

public class DefaultRecursiveRewriterVisitor implements
		RecursiveRewriterVisitor {

	@Override
	public void visitBegin(AlterTableAddColumnStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableAddConstraintStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableAlterColumnStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableChangeColumnStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableDropColumnStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableDropConstraintStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableModifyColumnStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableOrderByStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableRenameTableStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AlterTableStatement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(CreateIndex n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ColDataType n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ColumnDefinition n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(CreateTable n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Delete n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Drop n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(DropIndex n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(DropTable n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Insert n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(RenameTable n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Replace n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AllColumns n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AllTableColumns n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ColumnIndex n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Distinct n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(FromItem n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Join n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Limit n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(OrderByElement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(PlainSelect n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Select n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(SelectExpressionItem n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(SelectItem n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(SubJoin n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(SubSelect n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Top n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Union n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(WithItem n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Statement n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Truncate n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Update n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object visitEnd(AlterTableAddColumnStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableAddConstraintStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableAlterColumnStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableChangeColumnStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableDropColumnStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableDropConstraintStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableModifyColumnStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableOrderByStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableRenameTableStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AlterTableStatement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(CreateIndex n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ColDataType n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ColumnDefinition n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(CreateTable n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Delete n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Drop n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(DropIndex n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(DropTable n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Insert n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(RenameTable n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Replace n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AllColumns n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AllTableColumns n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ColumnIndex n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Distinct n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(FromItem n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Join n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Limit n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(OrderByElement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(PlainSelect n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Select n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(SelectExpressionItem n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(SelectItem n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(SubJoin n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(SubSelect n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Top n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Union n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(WithItem n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Statement n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Truncate n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Update n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void visitBegin(AllComparisonExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AnyComparisonExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(CaseExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(DateValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(DoubleValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Function n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(InverseExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(JdbcParameter n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(LongValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(NullValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Addition n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Division n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Multiplication n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Subtraction n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(AndExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(OrExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Between n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(EqualsTo n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ExistsExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ExpressionList n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(GreaterThan n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(GreaterThanEquals n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(InExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(IsNullExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(ItemsList n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(LikeExpression n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(MinorThan n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(MinorThanEquals n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(NotEqualsTo n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Parenthesis n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(StringValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(TimestampValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(TimeValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(UnaryValue n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(WhenClause n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object visitEnd(AllComparisonExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AnyComparisonExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(CaseExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(DateValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(DoubleValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Function n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(InverseExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(JdbcParameter n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(LongValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(NullValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Addition n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Division n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Multiplication n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Subtraction n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(AndExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(OrExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Between n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(EqualsTo n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ExistsExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ExpressionList n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(GreaterThan n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(GreaterThanEquals n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(InExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(IsNullExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(ItemsList n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(LikeExpression n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(MinorThan n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(MinorThanEquals n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(NotEqualsTo n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Parenthesis n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(StringValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(TimestampValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(TimeValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(UnaryValue n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(WhenClause n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void visitBegin(Column n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Table n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object visitEnd(Column n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object visitEnd(Table n) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pushASTContext(ASTContext c) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void popASTContext() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visitBegin(Concatenation n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object visitEnd(Concatenation n) {
		// TODO Auto-generated method stub
		return null;
	}


}