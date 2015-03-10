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
 
package com.relationalcloud.tsqlparser.statement.select;

import java.util.List;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.visitors.recursive.ASTContext;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 */
/**
 * The core of a "SELECT" statement (no UNION, no ORDER BY) 
 */
public class PlainSelect implements SelectBody {
	private Distinct distinct = null;
	private List selectItems;
	private Table into;
	private FromItem fromItem;
	private List joins;
	private Expression where;
	private List groupByColumnReferences;
	private List orderByElements;
	private Expression having;
	private Limit limit;
	private Top top;
	private boolean isForUpdate;
	
	/**
	 * The {@link FromItem} in this query
	 * @return the {@link FromItem}
	 */
	public FromItem getFromItem() {
		return fromItem;
	}

	public Table getInto() {
		return into;
	}

	/**
	 * The {@link SelectItem}s in this query (for example the A,B,C in "SELECT A,B,C")
	 * @return a list of {@link SelectItem}s
	 */
	public List getSelectItems() {
		return selectItems;
	}

	public Expression getWhere() {
		return where;
	}

	public void setFromItem(FromItem item) {
		fromItem = item;
	}

	public void setInto(Table table) {
		into = table;
	}


	public void setSelectItems(List list) {
		selectItems = list;
	}

	public void setWhere(Expression where) {
		this.where = where;
	}

	
	/**
	 * The list of {@link Join}s
	 * @return the list of {@link Join}s
	 */
	public List getJoins() {
		return joins;
	}

	public void setJoins(List list) {
		joins = list;
	}

	public void accept(SelectVisitor selectVisitor){
		selectVisitor.visit(this);
	}

	public List getOrderByElements() {
		return orderByElements;
	}

	public void setOrderByElements(List orderByElements) {
		this.orderByElements = orderByElements;
	}

	public Limit getLimit() {
		return limit;
	}

	public void setLimit(Limit limit) {
		this.limit = limit;
	}

	public Top getTop() {
		return top;
	}

	public void setTop(Top top) {
		this.top = top;
	}

	public Distinct getDistinct() {
		return distinct;
	}

	public void setDistinct(Distinct distinct) {
		this.distinct = distinct;
	}

	public Expression getHaving() {
		return having;
	}

	public void setHaving(Expression expression) {
		having = expression;
	}

	/**
	 * A list of {@link ColumnReference}s of the GROUP BY clause.
	 * It is null in case there is no GROUP BY clause
	 * @return a list of {@link ColumnReference}s 
	 */
	public List getGroupByColumnReferences() {
		return groupByColumnReferences;
	}

	public void setGroupByColumnReferences(List list) {
		groupByColumnReferences = list;
	}

	public String toString() {
		String sql = "";

		sql = "SELECT ";
		sql += ((distinct != null)?""+distinct+" ":"");
		sql += ((top != null)?""+top+" ":"");
		sql += getStringList(selectItems);
		sql += " FROM " + fromItem;
		sql += getFormatedList(joins, "", false, false);
		sql += ((where != null) ? " WHERE " + where : "");
		sql += getFormatedList(groupByColumnReferences, "GROUP BY");
		sql += ((having != null) ? " HAVING " + having : "");
		sql += orderByToString(orderByElements);
		sql += ((limit != null) ? limit+"" : "");

		return sql;
	}


	

	public static String orderByToString(List orderByElements) {
		return getFormatedList(orderByElements, "ORDER BY");
	}

	
	public static String getFormatedList(List list, String expression) {
		return getFormatedList(list, expression, true, false);
	}

	
	public static String getFormatedList(List list, String expression, boolean useComma, boolean useBrackets) {
		String sql = getStringList(list, useComma, useBrackets);

		if (sql.length() > 0) {
		    if (expression.length() > 0) {
		        sql = " " + expression + " " + sql;
		    } else { 
		        sql = " " + sql;
		    }
		}

		return sql;
	}

	/**
	 * List the toString out put of the objects in the List comma separated. If
	 * the List is null or empty an empty string is returned.
	 * 
	 * The same as getStringList(list, true, false)
	 * @see #getStringList(List, boolean, boolean)
	 * @param list
	 *            list of objects with toString methods
	 * @return comma separated list of the elements in the list
	 */
	public static String getStringList(List list) {
		return getStringList(list, true, false);
	}

	/**
	 * List the toString out put of the objects in the List that can be comma separated. If
	 * the List is null or empty an empty string is returned.
	 * 
	 * @param list list of objects with toString methods
	 * @param useComma true if the list has to be comma separated
	 * @param useBrackets true if the list has to be enclosed in brackets
	 * @return comma separated list of the elements in the list
	 */
	public static String getStringList(List list, boolean useComma, boolean useBrackets) {
		String ans = "";
		String comma = ",";
		if (!useComma) {
		    comma = "";
		}
		if (list != null) {
		    if (useBrackets) {
		        ans += "(";
		    }
		    
			for (int i = 0; i < list.size(); i++) {
				ans += "" + list.get(i) + ((i < list.size() - 1) ? comma + " " : "");
			}
			
		    if (useBrackets) {
		        ans += ")";
		    }
		}

		return ans;
	}

	public void setIsForupdate(boolean b) {
		isForUpdate=b;		
	}

	public boolean isForUpdate() {
		return isForUpdate;
	}

	@Override
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		
		v.pushASTContext(ASTContext.FROM);
		{
			fromItem.accept(v);
			List<Join> joinItems = getJoins();
			if (joinItems != null)
				for (Join j : joinItems)
					j.accept(v);
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.WHERE);
		{
			if (where != null)
				where.accept(v);
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.GROUP_BY);
		{
			if (groupByColumnReferences != null)
				for (ColumnReference c : (List<ColumnReference>) groupByColumnReferences)
					c.accept(v);
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.ORDER_BY);
		{
			List<OrderByElement> elems = getOrderByElements();
			if (elems != null) {
				for (OrderByElement e : elems)
					e.accept(v);
				if (having != null)
					having.accept(v);
			}
		}
		v.popASTContext();

		// TODO: limit
		// TODO: top
		
		v.pushASTContext(ASTContext.PROJECTIONS);
		{
			if (distinct != null)
				distinct.accept(v);
			List<SelectItem> selectItems = getSelectItems();
			for (SelectItem s : selectItems)
				s.accept(v);
		}
		v.popASTContext();
		
		v.visitEnd(this);
	}

	@Override
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		
		v.pushASTContext(ASTContext.FROM);
		{
			Object f = fromItem.accept(v);
			if (f != null)
				fromItem = (FromItem) f;
			List<Join> joinItems = getJoins();
			if (joinItems != null)
				for (int i = 0; i < joinItems.size(); i++) {
					Object j = joinItems.get(i).accept(v);
					if (j != null)
						joinItems.set(i, (Join) j);
				}
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.WHERE);
		{
			if (where != null) {
				Object w = where.accept(v);
				if (w != null)
					where = (Expression) w;
			}
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.GROUP_BY);
		{
			if (groupByColumnReferences != null) {
				List<ColumnReference> refs = (List<ColumnReference>) groupByColumnReferences;
				for (int i = 0; i < refs.size(); i++) {
					Object c = refs.get(i).accept(v);
					if (c != null)
						refs.set(i, (ColumnReference) c);
				}
			}
		}
		v.popASTContext();
		
		v.pushASTContext(ASTContext.ORDER_BY);
		{
			List<OrderByElement> elems = getOrderByElements();
			if (elems != null) {
				for (int i = 0; i < elems.size(); i++) {
					Object o = elems.get(i).accept(v);
					if (o != null)
						elems.set(i, (OrderByElement) o);
				}
				if (having != null) {
					Object h = having.accept(v);
					if (h != null)
						having = (Expression) h;
				}
			}
		}
		v.popASTContext();

		// TODO: limit
		// TODO: top
		
		v.pushASTContext(ASTContext.PROJECTIONS);
		{
			if (distinct != null) {
				Object d = distinct.accept(v);
				if (d != null)
					distinct = (Distinct) d;
			}
			List<SelectItem> selectItems = getSelectItems();
			for (SelectItem s : selectItems)
				s.accept(v);
		}
		v.popASTContext();
		return v.visitEnd(this);
	}
	
	@Override
	public PlainSelect getRepresentativePlainSelect() {
		return this;
	}
	
}