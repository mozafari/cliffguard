package edu.umich.robustopt.workloads;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.Function;
import com.relationalcloud.tsqlparser.expression.operators.conditional.AndExpression;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.select.FromItem;
import com.relationalcloud.tsqlparser.statement.select.Join;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.DefaultRecursiveRewriterVisitor;

import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaConnection;

public class QueryPruner {

	private final Connection conn;
	public QueryPruner(Connection conn) {
		this.conn = conn;
	}
	
	public static <T> List<T> ListConcat(List<T> a, List<T> b) {
		List<T> ret = new ArrayList<T>(a);
		ret.addAll(b);
		return ret;
	}
	
	private static List<Expression> TopLevelConjuncts(Expression e) {
		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			return ListConcat(TopLevelConjuncts(a.getLeftExpression()), TopLevelConjuncts(a.getRightExpression()));
		}
		return Collections.singletonList(e);
	}
	
	private static Expression AndConjuncts(List<Expression> e) {
		if (e.isEmpty())
			throw new IllegalArgumentException("no conjunctions given");
		Expression ret = e.get(0);
		for (int i = 1; i < e.size(); i++)
			ret = new AndExpression(ret, e.get(i));
		return ret;
	}
	
	private static PlainSelect FormatForSelectivityEstimation(PlainSelect orig, Expression clause) {
		// estimate the selectivity of clause in orig. 
		//
		// currently, this works by replacing the SELECT clause with COUNT(*), the
		// WHERE clause with clause, and removing GROUP BY / ORDER BY / LIMIT
		
		FromItem fi = orig.getFromItem();
		List<Join> jis = orig.getJoins();
		
		PlainSelect ps = new PlainSelect();
		Function countStar = new Function();
		countStar.setAllColumns(true);
		countStar.setName("count");
		ps.setSelectItems(Collections.singletonList(countStar));
		ps.setFromItem(fi);
		ps.setJoins(jis);
		ps.setWhere(clause);
		
		return ps;
	}
	
	private class PruneRewriter extends DefaultRecursiveRewriterVisitor {
		private Object rewritePlainSelect(PlainSelect ps) {
			Expression w = ps.getWhere();
			if (w == null)
				return null;
			// look at all top level conjuncts in where
			List<Expression> cs = TopLevelConjuncts(w);
			List<Expression> newCS = new ArrayList<Expression>();
			
			java.sql.Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = conn.createStatement();
				for (Expression c : cs) {
					PlainSelect q = FormatForSelectivityEstimation(ps,  c);
					String sql = q.toString();
					System.out.println("exec: " + sql);
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						int count = rs.getInt(1);
						if (count > 0) 
							newCS.add(c);
						break;
					}
					rs.close();
					rs = null;
				}
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				newCS.clear();
				e.printStackTrace();
			} finally {
					try {
						if (rs != null)
							rs.close();
						if (stmt != null)
							stmt.close();
					} catch (SQLException e) {
						newCS.clear();
						e.printStackTrace();
						// can't close, so quit trying
					}
			}
			
			if (newCS.isEmpty()) {
				System.out.println("[WARNING]: Could not modify query to return results- returning original");
				System.out.println(ps.toString());
				return null;
			}
			
			PlainSelect newPs = new PlainSelect();
			newPs.setDistinct(ps.getDistinct());
			newPs.setFromItem(ps.getFromItem());
			newPs.setGroupByColumnReferences(ps.getGroupByColumnReferences());
			newPs.setHaving(ps.getHaving());
			newPs.setInto(ps.getInto());
			newPs.setIsForupdate(ps.isForUpdate());
			newPs.setJoins(ps.getJoins());
			newPs.setLimit(ps.getLimit());
			newPs.setOrderByElements(ps.getOrderByElements());
			newPs.setSelectItems(ps.getSelectItems());
			newPs.setTop(ps.getTop());
			newPs.setWhere(AndConjuncts(newCS));
			return newPs;
		}
		
		@Override
		public Object visitEnd(PlainSelect ps) {
			return rewritePlainSelect(ps);
		}
	}
	
	public Statement pruneQuery(Statement stmt) {
		PruneRewriter pr = new PruneRewriter();
		Object s = stmt.accept(pr);
		return s == null ? stmt : (Statement) s;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Connection conn = VerticaConnection.createConnection("localhost", 5433, "dataset1", "someUser", "passwd");
		QueryPruner qp = new QueryPruner(conn);
		
		String sql = "select a.ident_2251, case when a.ident_1035 > 0 then '79c' else '+nU' end as With_Loss_Amount,  case when a.ident_1763 = 'HqM' then '79c' else '+nU' end as ShortSale,  count(*) from st_etl_2.ident_91 a join st_etl_2.ident_91 b on a.ident_1187 = b.ident_1187 where a.ident_1980 = '+nU' and b.ident_1980 in ('PJs','Bf6') group by a.ident_2251 order by a.ident_2251";
		Parser p = new Parser("public", null, sql);
		Statement stmt = p.stmt;
		System.out.println(qp.pruneQuery(stmt));
	}

}
