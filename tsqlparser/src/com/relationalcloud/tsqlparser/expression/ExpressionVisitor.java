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


public interface ExpressionVisitor {
	public void visit(NullValue nullValue);
	public void visit(Function function);
	public void visit(InverseExpression inverseExpression);
	public void visit(JdbcParameter jdbcParameter);
	public void visit(DoubleValue doubleValue);
	public void visit(LongValue longValue);
	public void visit(DateValue dateValue);
	public void visit(TimeValue timeValue);
	public void visit(TimestampValue timestampValue);
	public void visit(Parenthesis parenthesis);
	public void visit(StringValue stringValue);
	public void visit(Addition addition);
	public void visit(Division division);
	public void visit(Multiplication multiplication);
	public void visit(Subtraction subtraction);
	public void visit(AndExpression andExpression);
	public void visit(OrExpression orExpression);
	public void visit(Between between);
	public void visit(EqualsTo equalsTo);
	public void visit(GreaterThan greaterThan);
	public void visit(GreaterThanEquals greaterThanEquals);
	public void visit(InExpression inExpression);
	public void visit(IsNullExpression isNullExpression);
	public void visit(LikeExpression likeExpression);
	public void visit(Concatenation concatenationExpression);
	public void visit(MinorThan minorThan);
	public void visit(MinorThanEquals minorThanEquals);
	public void visit(NotEqualsTo notEqualsTo);
	public void visit(Column tableColumn);
	public void visit(SubSelect subSelect);
	public void visit(CaseExpression caseExpression);
	public void visit(WhenClause whenClause);
	public void visit(ExistsExpression existsExpression);
	public void visit(AllComparisonExpression allComparisonExpression);
	public void visit(AnyComparisonExpression anyComparisonExpression);


}
