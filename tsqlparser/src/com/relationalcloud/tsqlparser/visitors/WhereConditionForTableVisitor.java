package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.Statement;

public class WhereConditionForTableVisitor {

	/**
	 * Assumes the predicates are in AND... and extract those that are expressed locally to the tablename... useful to decompose joins
	 * @param stmt
	 * @param tablename
	 * @return
	 * @throws Exception 
	 */
	  public ArrayList<BinaryExpression> getWhereForTableCondition(Statement stmt, String tablename,Schema schema) throws Exception {
		  
		  tablename = tablename.replaceAll("`","");
		  
		  ArrayList<String> list = new ArrayList<String>();
		  list.add(tablename);
		  
		  WhereConditionVisitor v = new WhereConditionVisitor();
		  Expression e = v.getWhereCondition(stmt);
		  ExtractSelectionPredicateVisitor ex = new ExtractSelectionPredicateVisitor();
		  ArrayList<BinaryExpression> be =  ex.getSelectionPredicate(e);
		  
		  
		  
		  ArrayList<BinaryExpression> retVal = new ArrayList<BinaryExpression>();
		  
		  for(BinaryExpression b:be){

			  if(!((b.getLeftExpression() instanceof Column && 
			     !(((Column)b.getLeftExpression()).getTable(list,schema)!=null && ((Column)b.getLeftExpression()).getTable(list,schema).getName().equals(tablename))) ||
			     (b.getRightExpression() instanceof Column && 
					     !((((Column)b.getRightExpression()).getTable(list,schema)!= null) && ((Column)b.getRightExpression()).getTable(list,schema).getName().equals(tablename)))))
				  retVal.add(b);
				  
		  }
			  
		  return retVal;
		  
	  }
}
