package com.relationalcloud.tsqlparser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.text.*;


import com.relationalcloud.tsqlparser.visitors.ChangeInExpVisitor;
import com.relationalcloud.tsqlparser.exception.NotImplementedException;
import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.TimestampValue;
import com.relationalcloud.tsqlparser.expression.operators.relational.EqualsTo;
import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.InExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsList;
import com.relationalcloud.tsqlparser.loader.IntegrityConstraint;
import com.relationalcloud.tsqlparser.loader.PrimaryKey;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.parser.CCJSqlParser;
import com.relationalcloud.tsqlparser.parser.ParseException;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.Index;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.schema.datatypes.TimestampDataType;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.update.Update;
import com.relationalcloud.tsqlparser.visitors.ExtractExpressionLogicVisitor;
import com.relationalcloud.tsqlparser.visitors.ExtractInExpressionsVisitor;
import com.relationalcloud.tsqlparser.visitors.ExtractSelectionPredicateVisitor;
import com.relationalcloud.tsqlparser.visitors.LimitVisitor;
import com.relationalcloud.tsqlparser.visitors.OrderByVisitor;
import com.relationalcloud.tsqlparser.visitors.TablesForStatementVisitor;
import com.relationalcloud.tsqlparser.visitors.ValuesListVisitor;
import com.relationalcloud.tsqlparser.visitors.WhereConditionForTableVisitor;
import com.relationalcloud.tsqlparser.visitors.WhereConditionVisitor;
import com.relationalcloud.tsqlparser.visitors.WhereJoinVisitor;
import com.relationalcloud.tsqlparser.expression.StringValue;

/**
 * SQL Parser
 * 
 * KNOWN LIMITATIONS:
 * "INSERT VALUES" are supported with only 1 row at a time getPredicate for
 * "INSERT VALUES" statements required the columns to be explicitly mentioned in
 * the statement (since the parser used not to have prior knowledge of the schema, this should be an easy fix). 
 * 
 * @param schema
 * @param sql
 */

public class Parser {

	public Statement stmt;
	public String schemaname;
	public String sql;
	public ArrayList<String> tablelist_string;
	public ArrayList<Table> tablelist;
	public Schema schema;
	public boolean ignoreCase;
	
	public Parser(String schemaname, Schema schema, String sql) throws ParseException {
		this(schemaname, schema, sql, true);
	}
	
	public Parser(String schemaname, Schema schema, String sql, boolean ignoreCase) throws ParseException {
		
		this.ignoreCase = ignoreCase;
		this.schema=schema;
		this.schemaname = (this.ignoreCase? schemaname.toLowerCase() : schemaname);
		

		// HACK I'm invoking a function removing all the SQL we can't parse.. this
		// should be remove once we have a complete parser
		sql = ParserHacks.fixSql(sql);
		this.sql=(this.ignoreCase? sql.toLowerCase() : sql);

		StringReader strReader = new StringReader(sql);
		CCJSqlParser simple = new CCJSqlParser(strReader);
		try {
			stmt = simple.statement();
		} catch (ParseException pe) {

		//	System.err.println("PROBLEM PARSING:" + sql);
			throw pe;

		}
		catch (OutOfMemoryError e)
		{
			System.err.println("[ Out of memory ]" + sql);
		}

	}

	private void computeTableList() {
		if (tablelist_string == null) {
			tablelist_string = new ArrayList<String>();
			TablesForStatementVisitor v = new TablesForStatementVisitor();
			tablelist = v.getAffectedTablesFromStatement(stmt);
			for (Table t : tablelist) {
				tablelist_string.add(t.getName());
			}
		}
	}

	public ArrayList<Table> getTableList() {

		computeTableList();
		return tablelist;

	}

	public ArrayList<String> getTableStringList() {

		computeTableList();
		return tablelist_string;

	}

	/**
	 * This methods returns all the WHERE clauses which are not joins and the
	 * couples column-value for INSERT statements
	 * @throws NotImplementedException 
	 */
	public ArrayList<BinaryExpression> getBinaryPredicates() throws NotImplementedException {
		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;
			ItemsList val = ins.getItemsList();
			ValuesListVisitor vlv = new ValuesListVisitor();
			java.util.List le = vlv.getListValue(val);


			java.util.List<Column> col = getColumnsForInsert(ins);

			for (int i = 0; i < Math.min(col.size(),le.size()); i++) {
				EqualsTo b = new EqualsTo();
				Column c = col.get(i);
				c.setTable(ins.getTable());
				b.setLeftExpression(c);
				b.setRightExpression((Expression) le.get(i));
				ret.add(b);
			}
		}

		else {

			// EXTRACT WHERE CLAUSE IF ANY
			WhereConditionVisitor v = new WhereConditionVisitor();
			Expression exp = v.getWhereCondition(stmt);
			ExtractSelectionPredicateVisitor espv = new ExtractSelectionPredicateVisitor();

			// EXTRACT ALL THE EXPRESSIONS
			if (exp != null) {
				ArrayList<BinaryExpression> are = espv.getSelectionPredicate(exp);
				for (BinaryExpression be : are) {

					// VERIFY THAT AT LEAST ONE OF THE TWO IS NOT A COLUMN
					if (!(be.getLeftExpression() instanceof Column)) {
						// IF THE QUERY IS ON A SINGLE TABLE WE CAN SIMPLY ADD TABLE INFO TO
						// THE BINARY EXPR
						if (((Column) be.getRightExpression()).getTable() == null) {
							if (tablelist.size() == 1)
								// set the schemaname in Table to null, to avoid WHERE clauses
								// with the schema in them
								((Column) be.getRightExpression()).setTable(tablelist.get(0));
						}
						ret.add(be);
					}
					if (!(be.getRightExpression() instanceof Column)) {
						// IF THE QUERY IS ON A SINGLE TABLE WE CAN SIMPLY ADD TABLE INFO TO
						// THE BINARY EXPR
						if (((Column) be.getLeftExpression()).getTable() == null) {
							if (tablelist.size() == 1)
								((Column) be.getLeftExpression()).setTable(tablelist.get(0));
						}
						ret.add(be);
					}
				}
			}
		}
		return ret;
	}


	public ArrayList<BinaryExpression> getAllBinaryPredicates() throws NotImplementedException {
		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		ExtractSelectionPredicateVisitor espv = new ExtractSelectionPredicateVisitor();

		// EXTRACT ALL THE EXPRESSIONS
		if (exp != null) {
			ArrayList<BinaryExpression> are = espv.getSelectionPredicate(exp);
			for (BinaryExpression be : are) {
				ret.add(be);
			}
		}

		return ret;
	}


	public void setInExpressionValues(InExpression ie, ItemsList il){
		ChangeInExpVisitor cie = new ChangeInExpVisitor();
		cie.changeInExpression(ie, il);
	}
	
	public ArrayList<InExpression> getAllInExpressions(){
		ArrayList<InExpression> ret = new ArrayList<InExpression>();
		computeTableList();

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		
		ExtractInExpressionsVisitor eiv = new ExtractInExpressionsVisitor();

		// EXTRACT ALL THE EXPRESSIONS
		if (exp != null) {
			ArrayList<InExpression> are = eiv.getInExpressions(exp);
			for (InExpression ie : are) {
				ret.add(ie);
			}
		}

		return ret;
	}

	// returns true if there is an OR, otherwise false
	public boolean expressionIncludesOr(){

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		ExtractExpressionLogicVisitor espv = new ExtractExpressionLogicVisitor();
		if(exp != null){
			return espv.selectionPredicateIncludesOr(exp);
		}
		return false;
	}


	public java.util.List<Column> getColumnsForInsert(Insert ins) {
		java.util.List<Column> col = ins.getColumns();
		if(col == null){
			String tablename =ins.getTable().getName().replaceAll("`","");
			com.relationalcloud.tsqlparser.loader.SchemaTable t = schema.getTable(tablename);
			Vector<String> v = t.getColumns(); 
			col = new ArrayList<Column>();
			for(String s:v){
				col.add(new Column(ins.getTable(), s));  
			}
		}
		return col;
	}



	public String getCountEquivalent() {

		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		String output = "SELECT COUNT(*) FROM ";

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;
			java.util.List<Column> col = this.getColumnsForInsert(ins);
			ItemsList val = ins.getItemsList();
			ValuesListVisitor vlv = new ValuesListVisitor();
			java.util.List le = vlv.getListValue(val);

			ins.getTable().setAlias("t");

			for (int i = 0; i < Math.min(col.size(),le.size()); i++) {
				EqualsTo b = new EqualsTo();
				Column c = col.get(i);
				c.setTable(ins.getTable());
				b.setLeftExpression(c);
				b.setRightExpression((Expression) le.get(i));

				ret.add(b);
			}
			output += ins.getTable() + " WHERE ";

			for (BinaryExpression b : ret) {
				output += b.getLeftExpression().toString() + "="
				+ b.getRightExpression().toString() + " AND ";
			}
			output = output.substring(0, output.length() - 5);
			return output;
		}

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);

		for (Table t : tablelist) {
			output += t + ", ";
		}
		output = output.substring(0, output.length() - 2);
		if (exp != null) {

			output += " WHERE " + exp;
		}
		return output;

	}


	public List getSelectTargetList() {

		if(stmt instanceof Select){
			return ((PlainSelect)((Select)stmt).getSelectBody()).getSelectItems();
		}

		return null;
	}

	public ArrayList<String[]> getMigrationStatement() {

		ArrayList<String[]> retval = new ArrayList<String[]>();

		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		String output = "SELECT * FROM ";

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;
			java.util.List<Column> col = this.getColumnsForInsert(ins);
			ItemsList val = ins.getItemsList();
			ValuesListVisitor vlv = new ValuesListVisitor();
			java.util.List le = vlv.getListValue(val);

			ins.getTable().setAlias("t");

			for (int i = 0; i < Math.min(col.size(),le.size()); i++) {
				EqualsTo b = new EqualsTo();
				Column c = col.get(i);
				c.setTable(ins.getTable());
				b.setLeftExpression(c);
				b.setRightExpression((Expression) le.get(i));

				ret.add(b);
			}
			output += ins.getTable() + " WHERE ";

			for (BinaryExpression b : ret) {
				output += b.getLeftExpression().toString() + "="
				+ b.getRightExpression().toString() + " AND ";
			}
			output = output.substring(0, output.length() - 5);

			String[] temp={output,ins.getTable().getName()};
			retval.add(temp);

			return retval;



		}



		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		ExtractSelectionPredicateVisitor espv = new ExtractSelectionPredicateVisitor();
		ArrayList<BinaryExpression> are =null;
		if(exp!=null)
			are = espv.getSelectionPredicate(exp);

		for (Table t : tablelist) {
			output += t;
			// EXTRACT ALL THE EXPRESSIONS
			if (exp != null) {
				ArrayList<BinaryExpression> matching = new ArrayList<BinaryExpression>();

				for (BinaryExpression be : are) {

					if (!(be.getLeftExpression() instanceof Column)) {
						if( ((Column) be.getRightExpression()).getTable()!=null && 
								((Column) be.getRightExpression()).getTable().getName()!=null &&
								((Column) be.getRightExpression()).getTable().getName().equals(t.getName()) 
								|| 
								(((Column) be.getRightExpression()).getTable()==null || ((Column) be.getRightExpression()).getTable().getName()==null) && 
								schema.containsColumn(t.getName(), ((Column) be.getRightExpression()).getColumnName()))
							matching.add(be);
					}            if (!(be.getRightExpression() instanceof Column)) {
						if( ((Column) be.getLeftExpression()).getTable()!=null && 
								((Column) be.getLeftExpression()).getTable().getName()!=null &&
								((Column) be.getLeftExpression()).getTable().getName().equals(t.getName()) 
								|| 
								(((Column) be.getLeftExpression()).getTable()==null || ((Column) be.getLeftExpression()).getTable().getName()==null) && 
								schema.containsColumn(t.getName(), ((Column) be.getLeftExpression()).getColumnName()))
							matching.add(be);
					}
				}
				if(!matching.isEmpty()){
					output+= " WHERE ";
					for(BinaryExpression i:matching)
						output+=i + " AND ";
					output=output.substring(0, output.length() - 5);
				}
			} 
			String[] temp={output,t.getName()};
			retval.add(temp);
			output="SELECT * FROM ";
		}

		return retval;
	}

	public ArrayList<BinaryExpression> getSelectionPredicate(){

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		ExtractSelectionPredicateVisitor espv = new ExtractSelectionPredicateVisitor();
		ArrayList<BinaryExpression> are =null;
		if(exp!=null)
			are = espv.getSelectionPredicate(exp);

		return are;
	}


	public String getGlobalCountEquivalent() {

		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		String output = "SELECT COUNT(*) FROM ";

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;
			output += ins.getTable();
			if (ins.getTable().getAlias() != null)
				output += " " + ins.getTable().getAlias();

			return output;
		}

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereJoinCondition(v.getWhereCondition(stmt));

		for (Table t : tablelist) {
			output += t + ", ";

		}
		output = output.substring(0, output.length() - 2);
		if (exp != null) {

			output += " WHERE " + exp;
		}
		return output;

	}

	public ArrayList<String> getEquivalentSQLForIdExtraction(
			String transactionid, int queryid) {

		ArrayList<String> retval = new ArrayList<String>();

		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;

			String tabname = ins.getTable().toString();
			if (tabname.startsWith("`") && tabname.endsWith("`"))
				tabname = tabname.substring(1, tabname.length() - 1);

			String target_id = ins.getTable() + ".relcloud_id";

			String output = "SELECT \'" + transactionid + "\', " + queryid + ", \'"
			+ tabname + "\', " + target_id + ", " + "\'insert\' FROM ";

			java.util.List<Column> col = getColumnsForInsert(ins);
			ItemsList val = ins.getItemsList();
			ValuesListVisitor vlv = new ValuesListVisitor();
			java.util.List le = vlv.getListValue(val);



			for (int i = 0; i < Math.min(col.size(),le.size()); i++) {
				EqualsTo b = new EqualsTo();
				Column c = col.get(i);
				c.setTable(ins.getTable());
				b.setLeftExpression(c);
				b.setRightExpression((Expression) le.get(i));

				ret.add(b);
			}


			if (ins.getTable().toString().startsWith("`"))
				output += "`relcloud_" + ins.getTable().toString().substring(1);
			else
				output += "relcloud_" + ins.getTable().toString();

			output += " as " + ins.getTable() + " WHERE ";

			for (BinaryExpression b : ret) {
				output += b.getLeftExpression().toString() + "="
				+ b.getRightExpression().toString() + " AND ";
			}
			output = output.substring(0, output.length() - 5);
			retval.add(output);
			return retval;
		}

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);

		for (Table tabname : tablelist) {

			if (tabname.getName().startsWith("`") && tabname.getName().endsWith("`"))
				tabname.setName(tabname.getName().substring(1,
						tabname.getName().length() - 1));

			String querytype = "select";
			if (stmt instanceof Update)
				querytype = "update";
			if (stmt instanceof Delete)
				querytype = "delete";

			String alias = "";
			if (tabname.getAlias() != null)
				alias = tabname.getAlias();
			else
				alias = tabname.getName();

			if(!alias.startsWith("`"))
				alias = "`"+alias+"`";

			String target_id = alias + ".relcloud_id";

			String output = "SELECT \'" + transactionid + "\', " + queryid + ", \'"
			+ tabname.getName() + "\', " + target_id + ", \'" + querytype
			+ "\' FROM ";

			for (Table from : tablelist) {

				String ttname = from.getName();  
				if(from.getName().startsWith("`"))  
					ttname=from.getName().substring(1,from.getName().length()-1);

				if (tabname.getAlias() != null)
					output += "relcloud_" + ttname + " as " + from.getAlias() + ", ";
				else
					output += "relcloud_" + ttname + " as `" + ttname + "`, ";

			}
			output = output.substring(0, output.length() - 2);
			if (exp != null) {
				output += " WHERE " + exp;
			}

			OrderByVisitor ob = new OrderByVisitor();
			List oblist = ob.getOrderBy(stmt);

			if (oblist != null) {
				output += PlainSelect.orderByToString(oblist);
			}

			LimitVisitor lim = new LimitVisitor();
			long limit = lim.getLimitValue(stmt);

			if (limit > 0)
				output += " LIMIT " + limit;

			retval.add(output);
		}

		return retval;

	}

	/**
	 * return true if the statement is not update,delete,insert
	 * 
	 * @return
	 */
	public boolean isRead() {

		if (stmt instanceof Update || stmt instanceof Delete
				|| stmt instanceof Insert)
			return false;

		return true;
	}

	public Schema getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

	public ArrayList<BinaryExpression> getJoinPredicates() {


		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);
		WhereJoinVisitor v2 = new WhereJoinVisitor();
		HashMap<Column, Column> hm = v2.GetColumnFromJoin(exp);

		ArrayList<BinaryExpression>  ret = new ArrayList<BinaryExpression> ();

		for(Column c:hm.keySet()){
			EqualsTo e = new EqualsTo();
			e.setLeftExpression(c);
			e.setRightExpression(hm.get(c));
			ret.add(e);
		}

		return ret;

	}

	public List<ColumnDefinition> getCreateTableColumnDefinitions() {

		if(stmt instanceof CreateTable){
			return ((CreateTable)stmt).getColumnDefinitions();
		}
		return null;
	}

	public List<Index> getCreateTableConstraints() {

		if(stmt instanceof CreateTable){
			return ((CreateTable)stmt).getIndexes();
		}
		return null;
	}

	/**
	 * Currently rewrites only insert,update,delete with a single table. Select can have more than one table, but 
	 * @return
	 */
	public ArrayList<String> rewriteToTemporal() {
		
		String getTimeFunction="now()";
		String infiniteTime="'2037-12-31 23:59:59'";
		if(schema.DRIVER.contains("MySQL"))
		{
			getTimeFunction="now_usec()";
			infiniteTime="cast(20371231235959.000000 as DECIMAL(20,6))";
		}

		ArrayList<String> res = new ArrayList<String>();

		if(stmt instanceof Insert){
			Table tab = ((Insert)stmt).getTable();
			if(((Insert) stmt).getColumns() != null){
				((Insert) stmt).getColumns().add(new Column(null,"tstart"));
				((Insert) stmt).getColumns().add(new Column(null,"tend"));
			}


			// create a java calendar instance
			Calendar calendar = Calendar.getInstance();

			// get a java.util.Date from the calendar instance.
			// this date will represent the current instant, or "now".
			java.util.Date now = calendar.getTime();

			// a java current time (now) instance
			java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());    
			((ExpressionList)((Insert) stmt).getItemsList()).getExpressions().add(getTimeFunction);
			((ExpressionList)((Insert) stmt).getItemsList()).getExpressions().add(infiniteTime);

			res.add(stmt.toString());
			//System.out.println("In background: "+res);
			return res;
		}

		if(stmt instanceof Delete){
			res.add("UPDATE " + ((Delete)stmt).getTables().get(0).getName() + " SET tend="+getTimeFunction+" WHERE tend="+infiniteTime+" AND "+ ((Delete)stmt).getWhere());
			//System.out.println("In background: "+res);
			return res;
		}


		if(stmt instanceof Update){
			Table tab = (Table)((Update)stmt).getTables().get(0);
			Expression where = ((Update)stmt).getWhere();
			List column = ((Update)stmt).getColumns();

			for(Object c:column.toArray())
				((Column)c).setTable(tab);

			List expr = ((Update)stmt).getExpressions();
			Vector<String> allColumns = schema.getTable(tab.getName()).getColumns();

			// IMPORTANT ASSUMPTION: the order of UPDATE and INSERT matters for the code in PreparedStatementSpy! Do not change
			String tempTime=getTimeFunction;
			String swapVar="";
			// MySQL needs a temporary variable to keep a unique time throughout the transaction
			// Postgres's Now() returns the same timestamp within the same transaction

			if(schema.DRIVER.contains("MySQL"))
			{
				tempTime="@temptime";
				swapVar=" AND "+tempTime+":=convert(now_usec(), DECIMAL(20,6))";
			}
			res.add("UPDATE " + tab.getName() + " SET tend="+tempTime+" WHERE tend="+ infiniteTime+ " " + ((where!=null)? "AND " + where:"")+swapVar) ;
			String ins = "INSERT INTO "+tab.getName() +" (";

			String inslist = "";
			String sellist = "";

			for(String c:allColumns){

				if(!c.equalsIgnoreCase("tend")){

					inslist+=c + ",";	

					String temp=c;
					int i = 0;
					for(Object cc:column){
						if(((Column)cc).getColumnName().equalsIgnoreCase(c)){
							temp=expr.get(i).toString();
							break;
						}
						i++;
					}
					if(!temp.equalsIgnoreCase("tstart"))
						sellist+=temp + ",";					
				}
			}
			ins += inslist +"tend) SELECT " + sellist +" "+tempTime+", "+infiniteTime+" FROM " + tab.getName() +  " WHERE tend="+tempTime+" " + ((where!=null)? " AND " + where:"");
			res.add(ins);
			//System.out.println("In background: "+res);
			return res;
		}


		if(stmt instanceof Select){

			PlainSelect p = (PlainSelect)((Select)stmt).getSelectBody();


			TablesForStatementVisitor v = new TablesForStatementVisitor();
			String timecond = " ";
			for(Table t:v.getAffectedTablesFromStatement(stmt)){
				timecond +=  ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tend= "+infiniteTime+" AND ";
			}
			timecond = timecond.substring(0, timecond.length()-4);

			String sql = "";

			sql = "SELECT ";
			sql += ((p.getDistinct() != null)?""+p.getDistinct()+" ":"");
			sql += ((p.getTop() != null)?""+p.getTop()+" ":"");
			sql += PlainSelect.getStringList(p.getSelectItems());
			sql += " FROM " + p.getFromItem();
			sql += PlainSelect.getFormatedList(p.getJoins(), "", false, false);
			sql += " WHERE " + ((p.getWhere() != null) ? " ("+ p.getWhere() +") AND ": " ") +timecond;			
			sql += PlainSelect.getFormatedList(p.getGroupByColumnReferences(), "GROUP BY");
			sql += ((p.getHaving() != null) ? " HAVING " + p.getHaving() : "");
			sql += PlainSelect.orderByToString(p.getOrderByElements());
			sql += ((p.getLimit() != null) ? p.getLimit()+"" : "");
			res.add(sql);
			//System.out.println("In background: "+res);
			return res;
		}
		
		return null;
		
	}

	public String toNonTemporalSQL(){


		if(stmt instanceof Select){

			PlainSelect p = (PlainSelect)((Select)stmt).getSelectBody();

			TablesForStatementVisitor v = new TablesForStatementVisitor();
			String timecond = " ";
			for(Table t:v.getAffectedTablesFromStatement(stmt)){

				if(t.isTemporal()){
					String ts1=""+t.getTimestamp1();
					String ts2=""+t.getTimestamp2();
					if(schema.DRIVER.contains("MySQL"))
					{
						ts1=t.getTimestamp1().toDecimalTime();
						ts2=t.getTimestamp1().toDecimalTime();
					}
					//System.out.println(ts1+" "+ts2);
					if(timecond.length()>1)
						timecond=timecond+" AND ";
					if(t.isBefore())
						timecond += ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tend<='" +ts1+ "'";
					if(t.isAfter())
						timecond += ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tstart>'" +ts1 + "' AND " + ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tend>'" +ts1+"'";
					if(t.isAsOf())
						timecond += ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tstart<='" +ts1 + "' AND " + ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tend>'" +ts1+"'";
					if(t.isBetween())
						timecond += ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tstart<='" + ts2 + "' AND " + ((t.getAlias() != null) ? t.getAlias() : t.getName()) +".tend>'" +ts1+"'";

				}

			}
			
			//System.out.println(timecond);
			String sql = "";

			sql = "SELECT ";
			sql += ((p.getDistinct() != null)?""+p.getDistinct()+" ":"");
			sql += ((p.getTop() != null)?""+p.getTop()+" ":"");
			sql += PlainSelect.getStringList(p.getSelectItems());
			sql += " FROM " + p.getFromItem();
			sql += PlainSelect.getFormatedList(p.getJoins(), "", false, false);
			sql += ((p.getWhere() != null) ? " WHERE (" + p.getWhere() +") AND "+timecond+" ": " WHERE"+timecond+" ");
			sql += PlainSelect.getFormatedList(p.getGroupByColumnReferences(), "GROUP BY");
			sql += ((p.getHaving() != null) ? " HAVING " + p.getHaving() : "");
			sql += PlainSelect.orderByToString(p.getOrderByElements());
			sql += ((p.getLimit() != null) ? p.getLimit()+"" : "");

			//System.out.println("In background: "+sql);
			return sql;

		}



		return null;

	}

	public boolean isTemporal() {

		TablesForStatementVisitor v = new TablesForStatementVisitor();
		String timecond = " ";
		for(Table t:v.getAffectedTablesFromStatement(stmt)){
			if(t.isTemporal())
				return true;
		}
		return false;


	}

	public int getNonTemporalParametersCount() {

		int i=0;
		if(stmt instanceof Update){
			for(Object r:((Update)stmt).getExpressions().toArray()){
				Expression e = (Expression) r;
				if(e.toString().contains("?"))
					i++;
			}
		}

		return i;
	}
	
	/**
	 * Return an HashMap of table,query where the query would return all tuples (pk) accessed by the original query... Joins are treated optimistically if postJoin=true, i.e., we return only matching tuples in each table
	 * if postJoin=false we return all tuples that pass the "local" predicates at each table.
	 * @param postJoin
	 * @return
	 * @throws Exception
	 */
	public HashMap<String,String> getPrimaryKeyEquivalent(boolean postJoin) throws Exception {

		HashMap<String,String> returnVal = new HashMap<String,String>();
		 
		ArrayList<BinaryExpression> ret = new ArrayList<BinaryExpression>();
		computeTableList();
		
		String output = "SELECT ";

		if (stmt instanceof Insert && ((Insert) stmt).isUseValues()) {
			Insert ins = (Insert) stmt;
			java.util.List<Column> col = this.getColumnsForInsert(ins);
			ItemsList val = ins.getItemsList();
			ValuesListVisitor vlv = new ValuesListVisitor();
			java.util.List le = vlv.getListValue(val);

			ins.getTable().setAlias("t");
			
			
			
			for (int i = 0; i < Math.min(col.size(),le.size()); i++) {
				EqualsTo b = new EqualsTo();
				Column c = col.get(i);
				c.setTable(ins.getTable());
				b.setLeftExpression(c);
				
				Expression v = (Expression) le.get(i);
				
				if(v instanceof StringValue)
					v = new StringValue(((StringValue) v).toString().replaceAll("'", "\'"));
				b.setRightExpression((Expression) v);

				ret.add(b);
			}
			
			String tabname = ins.getTable().getName();
			
			tabname = tabname.replaceAll("`", "");
			
			if(schema.getTable(tabname).getPrimaryKey()==null)
				return null;
			
			for(String s:schema.getTable(tabname).getPrimaryKey())
				output+= s + ",";
			output = output.substring(0,output.length()-1);
			output += " FROM " + ins.getTable() + " WHERE ";

			Vector<String> pk = schema.getTable(tabname).getPrimaryKey();
			
			for (BinaryExpression b : ret) {
				if(pk.contains(((Column)b.getLeftExpression()).getColumnName())){
					output += b.getLeftExpression().toString() + "=" + b.getRightExpression().toString() + " AND ";
				}
			}
			output = output.substring(0, output.length() - 5);
			returnVal.put(ins.getTable().toString().replaceAll("`",""),output);
			return returnVal;
		}

		// EXTRACT WHERE CLAUSE IF ANY
		WhereConditionVisitor v = new WhereConditionVisitor();
		Expression exp = v.getWhereCondition(stmt);

		
		if(postJoin){
			for(Table t:tablelist){
				output = "SELECT ";
				String tablename = t.getName().replaceAll("`","");
				SchemaTable sct = schema.getTable(tablename);
				
				if(sct.getPrimaryKey()==null)
					return null;
				
				for(String s:sct.getPrimaryKey())
					output+= s + ",";
				output = output.substring(0,output.length()-1);
				
				output += " FROM ";
				
				for(Table s:tablelist)
					output+= s + ",";
				output = output.substring(0,output.length()-1);
				if(exp!=null){		
					output += " WHERE " + exp;
				}
				returnVal.put(t.getName().replaceAll("`",""),output);
			}
		}else{	
		
			// PESSIMISTIC VERSION ASSUMING WE TOUCH ALL TUPLES IN A JOIN		
			for (Table t : tablelist) {
				output = "SELECT ";
				String tablename = t.getName().replaceAll("`","");
				SchemaTable sct = schema.getTable(tablename);
				
				if(sct.getPrimaryKey()==null)
					return null;
				
				for(String s:sct.getPrimaryKey())
					output+= s + ",";
				output = output.substring(0,output.length()-1);
				output += " FROM " + t.getName();
	
				
				if (exp != null) {
					WhereConditionForTableVisitor v2 = new WhereConditionForTableVisitor();
					ArrayList<BinaryExpression> exp2 = v2.getWhereForTableCondition(stmt,t.getName(),schema);
					
					if(exp2 !=null){
					output += " WHERE ";
					for(BinaryExpression be:exp2)
						output+= be.toString() + " AND ";
					output = output.substring(0, output.length() - 5);
					}
				}
				returnVal.put(t.getName().replaceAll("`",""),output);
			}
			
		}
	
		return returnVal;

	}


}

	