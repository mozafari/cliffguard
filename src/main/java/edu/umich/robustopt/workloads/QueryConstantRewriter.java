package edu.umich.robustopt.workloads;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.ScalarExpression;
import com.relationalcloud.tsqlparser.expression.ScalarExpressionUtils;
import com.relationalcloud.tsqlparser.expression.operators.conditional.AndExpression;
import com.relationalcloud.tsqlparser.expression.operators.conditional.OrExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.EqualsTo;
import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.InExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.IsNullExpression;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsList;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThanEquals;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;
import com.relationalcloud.tsqlparser.parser.TokenMgrError;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.visitors.recursive.ASTContext;

import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.staticanalysis.DefaultSemanticallyAwareRewriterVisitor;
import edu.umich.robustopt.staticanalysis.SelectContext;
import edu.umich.robustopt.staticanalysis.SemanticAnalyzerVisitor;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public class QueryConstantRewriter {
	
	private final Map<String, Schema> schemas;
	private final ConstantValueManager constManager;
	
	public QueryConstantRewriter(String dbAlias, SchemaDescriptor schemaDesc, double samplingRate, String databaseLoginFile, String DBVendor) throws Exception {
		this.schemas = schemaDesc.getSchemas();
		this.constManager = new ConstantValueManager(dbAlias, samplingRate, databaseLoginFile, DBVendor);
	}
	

	
	public static class RewriteException extends RuntimeException {
		public RewriteException(String m) {
			super(m);
		}
		public RewriteException(Throwable t) {
			super(t);
		}
		public RewriteException(String m, Throwable t) {
			super(m, t);
		}
	}
	
	private class RewriterVisitor extends DefaultSemanticallyAwareRewriterVisitor {

		private final IdentityHashMap<PlainSelect, SelectContext> selectContexts;
		
		public RewriterVisitor(IdentityHashMap<PlainSelect, SelectContext> selectContexts) {
			super(QueryConstantRewriter.this.schemas);
			this.selectContexts = selectContexts;
		}
		
		@Override
		public void visitBegin(PlainSelect select) {
			SelectContext ctx = selectContexts.get(select);
			assert ctx != null;
			selectContextStack.push(ctx);
		}
		
		@Override
		public Object visitEnd(PlainSelect select) {
			SelectContext cur = currentSelectContext();
			assert cur == selectContexts.get(select);
			selectContextStack.pop();
			return null;
		}
		
		private ValueDistribution getValueDistFor(ColumnDescriptor cd) throws Exception {
			return constManager.getColumnDistribution(cd);
		}
		
		private class ExtractResult {
			public ExtractResult(boolean flip, ColumnDescriptor desc,
					Column column,
					ScalarExpression konst) {
				this.flip = flip;
				this.desc = desc;
				this.column = column;
				this.konst = konst;
			}
			public final boolean flip;
			public final ColumnDescriptor desc;
			public final Column column;
			public final ScalarExpression konst;

		}
		
		private ExtractResult getExprsFrom(BinaryExpression e) {
			// looking for column op const or const op column
			Expression l = e.getLeftExpression();
			Expression r = e.getRightExpression();
			
			boolean flip = false;
			Column col = null;
			ScalarExpression konst = null;
			if ((l instanceof Column) && (r instanceof ScalarExpression)) {
				flip = false;
				col = (Column) l;
				konst = (ScalarExpression) r;
			} else if ((l instanceof ScalarExpression) && (r instanceof Column)) {
				flip = true;
				col = (Column) r;
				konst = (ScalarExpression) l;
			}
			
			if (col == null || konst == null)
				return null;
			
			if (konst.literalValue() == null)
				return null;
			
			List<ColumnDescriptor> cds = currentSelectContext().getColumnDescriptors(col, schemas);
			if (cds.size() != 1)
				return null;
			
			return new ExtractResult(flip, cds.get(0), col, konst);
		}
		
		private BinaryExpression setExprs(
				BinaryExpression be, boolean flip, 
				Expression l, Expression r) {
			if (flip) {
				be.setLeftExpression(r);
				be.setRightExpression(l);
			} else {
				be.setLeftExpression(l);
				be.setRightExpression(r);
			}
			return be;
		}
		
		private IsNullExpression mkColumnIsNullExpr(Column c) {
			IsNullExpression e = new IsNullExpression();
			e.setLeftExpression(new Column(c));
			return e;
		}
		
		@Override
		public Object visitEnd(GreaterThanEquals geq) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			ExtractResult res = getExprsFrom(geq);
			if (res == null)
				return null;
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(res.desc);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(res.column);
			Object[] values = dist.findBounds(res.konst.literalValue());
			ScalarExpression konst0 = ScalarExpressionUtils.ConstructFrom(res.flip ? values[0] : values[1]);
			return setExprs(new GreaterThanEquals(), res.flip, new Column(res.column), konst0);
		}
		
		@Override
		public Object visitEnd(GreaterThan gt) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			ExtractResult res = getExprsFrom(gt);
			if (res == null)
				return null;
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(res.desc);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(res.column);
			Object[] values = dist.findBounds(res.konst.literalValue());
			ScalarExpression konst0 = ScalarExpressionUtils.ConstructFrom(res.flip ? values[0] : values[1]);
			return setExprs(new GreaterThan(), res.flip, new Column(res.column), konst0);
		}
		
		@Override
		public Object visitEnd(MinorThanEquals leq) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			ExtractResult res = getExprsFrom(leq);
			if (res == null)
				return null;
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(res.desc);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(res.column);
			Object[] values = dist.findBounds(res.konst.literalValue());
			ScalarExpression konst0 = ScalarExpressionUtils.ConstructFrom(res.flip ? values[1] : values[0]);
			return setExprs(new MinorThanEquals(), res.flip, new Column(res.column), konst0);
		}
		
		@Override
		public Object visitEnd(MinorThan lt) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			ExtractResult res = getExprsFrom(lt);
			if (res == null)
				return null;
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(res.desc);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(res.column);
			Object[] values = dist.findBounds(res.konst.literalValue());
			ScalarExpression konst0 = ScalarExpressionUtils.ConstructFrom(res.flip ? values[1] : values[0]);
			return setExprs(new MinorThan(), res.flip, new Column(res.column), konst0);
		}
		
		@Override
		public Object visitEnd(InExpression in) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			Expression l = in.getLeftExpression();
			ItemsList r = in.getItemsList();
			Column col = null;
			ExpressionList exprs = null;
			List<ScalarExpression> scalarExprs = new ArrayList<ScalarExpression>();
			
			if (!(l instanceof Column) || !(r instanceof ExpressionList))
				return null;
			col = (Column) l;
			exprs = (ExpressionList) r;
			
			for (Expression e : (List<Expression>) exprs.getExpressions()) {
				if (!(e instanceof ScalarExpression))
					return null;
				scalarExprs.add((ScalarExpression) e);
			}
			
			List<ColumnDescriptor> cds = currentSelectContext().getColumnDescriptors(col, schemas);
			if (cds.size() != 1)
				return null;
			//System.out.println("Found col: " + cds.get(0));
			ColumnDescriptor cd = cds.get(0);
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(cd);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(col);
			
			Expression e = null;
			for (ScalarExpression konst : scalarExprs) {
				Object[] b = dist.findBounds(konst.literalValue());
				// col >= const
				GreaterThanEquals lhs = new GreaterThanEquals();
				lhs.setLeftExpression(new Column(col));
				lhs.setRightExpression(ScalarExpressionUtils.ConstructFrom(b[0]));
				
				// col <= const
				MinorThanEquals rhs = new MinorThanEquals();
				rhs.setLeftExpression(new Column(col));
				rhs.setRightExpression(ScalarExpressionUtils.ConstructFrom(b[1]));
				
				AndExpression ae = new AndExpression(lhs, rhs);
				
				if (e == null)
					e = ae;
				else
					e = new OrExpression(e, ae);
			}
			assert e != null;
			return e;
		}
		
		@Override
		public Object visitEnd(EqualsTo eq) {
			if (currentASTContext() != ASTContext.WHERE)
				return null;
			
			// looking for column = const or const = column
			
			Column col;
			ScalarExpression konst;
			ColumnDescriptor cd;
			
			ExtractResult res = getExprsFrom(eq);
			if (res == null)
				return null;
			
			col = res.column;
			konst = res.konst;
			cd = res.desc;
		
			//System.out.println("Found col: " + cd);
			ValueDistribution dist = null;
			try {
				dist = getValueDistFor(cd);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (dist == null)
				return mkColumnIsNullExpr(col);
			Object[] b = dist.findBounds(konst.literalValue());
			
			// col >= const
			GreaterThanEquals lhs = new GreaterThanEquals();
			lhs.setLeftExpression(new Column(col));
			lhs.setRightExpression(ScalarExpressionUtils.ConstructFrom(b[0]));
			
			// col <= const
			MinorThanEquals rhs = new MinorThanEquals();
			rhs.setLeftExpression(new Column(col));
			rhs.setRightExpression(ScalarExpressionUtils.ConstructFrom(b[1]));
			
			return new AndExpression(lhs, rhs);
		}
	}
	
	public Statement rewrite(Statement stmt) {
		SemanticAnalyzerVisitor semanticAnalyzer = new SemanticAnalyzerVisitor(schemas);
		stmt.accept(semanticAnalyzer);
		IdentityHashMap<PlainSelect, SelectContext> contexts = semanticAnalyzer.getSelectContexts();
		RewriterVisitor rv = new RewriterVisitor(contexts);
		Object s = stmt.accept(rv);
		if (s != null)
			return (Statement) s;
		return stmt;
	}
	
	public static void main(String[] args) throws Exception {
		String DBVendor = VerticaDatabaseLoginConfiguration.class.getSimpleName();
		SchemaDescriptor desc = SchemaUtils.GetSchemaMapFromDefaultSources("tpch", DBVendor);
		QueryConstantRewriter r = new QueryConstantRewriter("tpch", desc, 0.05, null, DBVendor);
		
		boolean DoFile = true;
		
		if (DoFile) {
			File f = new File(GlobalConfigurations.RO_BASE_PATH, "dataset1/out_dc_requests_issued-scrubbed-selects");
			File of = new File(GlobalConfigurations.RO_BASE_PATH, "dataset1/out_dc_requests_issued-scrubbed-selects-rewritten");
			PrintWriter pw = new PrintWriter(new FileOutputStream(of));
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = null;
			int linenum = 1;
			while ((line = br.readLine()) != null) {
				int idx0 = line.indexOf('|');
				if (idx0 == -1) {
					
					throw new RuntimeException("malformed line: " + linenum);
				}
				String ts0 = line.substring(0, idx0);
				String query = line.substring(idx0 + 1);
				String queryToWrite = query;
				try {
					Parser p = new Parser("public", null, query);
					Statement stmt = p.stmt;
					if (stmt instanceof Select) {
						Statement stmt0 = r.rewrite(stmt);
						queryToWrite = stmt0.toString();
					}
				} catch (ParseException e) {
					//System.out.println("[PARSE] Error on line: " + linenum + ". Query: " + query);
				} catch (TokenMgrError e) {
					System.out.println("[PARSE/TOKEN] Error on line: " + linenum + ". Query: " + query);
				} catch (RewriteException e) {
					System.out.println("[REWRITE] Error on line: " + linenum + ". Query: " + query);
				} catch (Throwable t) {
					//throw new RuntimeException("Error on line: " + linenum + ". Query: " + query, t);
					System.out.println("[UNKNOWN] Error on line: " + linenum + ". Query: " + query);
				}
				
				pw.println(ts0 + "|" + queryToWrite);
				linenum++;
			}
			pw.close();
			br.close();
		} else {
			//Parser p = new Parser("public", null, "select  sum(case when a11.ident_2629='k56' then 1 else 0 end) AS ident_199,   sum(a11.ident_2090) AS ident_2272,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then age_in_months(a11.ident_421,a11.ident_225) * a11.ident_305 end) AS WJXBFS1,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_305) else null end) AS WJXBFS2,  sum(a11.ident_2386) AS ident_445,   sum(case when a11.ident_1980='e/M' then a11.ident_2090 else 0 end) AS ident_386,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then  a11.ident_305 end) AS WJXBFS3,   sum(case when a11.ident_1980 in ('EsE','HqM','Uew','e/M') then a11.ident_2090 else null end) AS ident_1869,   sum(a11.ident_552) AS CURRINVBAL1MAGO,  sum(Case when a11.ident_1612 in ('EsE','HqM','Uew','e/M') and a11.ident_2251=a11.ident_1817 then a11.ident_2471 end) AS PAYOFFBALANCECDR,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_341 * a11.ident_305) else  null end) AS WJXBFS4,   sum(case when a11.ident_2386 is not null and a11.ident_1779 in (6,14) then Case when a11.ident_2251=a11.ident_1817 then a11.ident_552 end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.ident_1980='Uew' then a11.ident_2090 else 0 end) AS ident_1637 from st_etl_2.ident_164  a11 where a11.ident_2251 in (278, 0) LIMIT 1000001");
			//Parser p = new Parser("public", null, "select * from st_etl_2.ident_164 a11 where a11.ident_2629='k56'");
			//Parser p = new Parser("public", null, "SELECT a12.ident_2051 AS ident_1019, max(a12.ident_2071) AS ident_2071, a11.ident_1351 AS ident_1351, a11.ident_471 AS ident_471, sum(a11.ident_2090) AS ident_2272 FROM st_etl_2.ident_164 AS a11 JOIN st_etl_2.ident_29 AS a12 ON ((a11.ident_1980 = a12.ident_1980)) WHERE ((((a11.ident_2251 IN (277) AND a12.ident_2051 IN (1, 2, 3, 4, 5, 6)) AND (a11.ident_1351 > 2000)) AND a11.ident_2224 BETWEEN 721 AND 740)) GROUP BY a12.ident_2051, a11.ident_1351, a11.ident_471 LIMIT 1000001");
			
			//String sql = "select a.ident_2251, case when a.ident_1035 > 0 then '79c' else '+nU' end as With_Loss_Amount,  case when a.ident_1763 = 'HqM' then '79c' else '+nU' end as ShortSale,  count(*) from st_etl_2.ident_91 a join st_etl_2.ident_91 b on a.ident_1187 = b.ident_1187 and a.ident_2251 = b.ident_2251 +1 where a.ident_1980 = '+nU' and b.ident_1980 in ('PJs','Bf6') group by a.ident_2251,  case when a.ident_1035 > 0 then '79c' else '+nU' end, case when a.ident_1763 = 'HqM' then '79c' else '+nU' end order by a.ident_2251";
			String sql = "SELECT a12.ident_2051 AS ident_1019, a11.ident_1612 AS ident_1612, sum(CASE WHEN (a11.ident_2189 > 0) THEN a11.ident_2189 ELSE a11.ident_2471 END) AS LASTBALANCE FROM st_etl_2.ident_164 AS a11 JOIN st_etl_2.ident_29 AS a12 ON ((a11.ident_1980 = a12.ident_1980)) WHERE (((((((((a11.ident_412) IN (SELECT c21.ident_412 FROM st_etl_2.ident_1 AS c21 WHERE ((c21.ident_2439 >= 'G4649512c   ') AND (c21.ident_2439 <= 'G4h70W708LoE')))) AND (a11.ident_1906 >= 120)) AND (a11.ident_1906 < 60)) AND (a11.ident_1797 > 180)) AND (a11.ident_1797 <= 120)) AND ((((((((((((a11.ident_1612 >= '+') AND (a11.ident_1612 <= 'E')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6)))) OR ((((a11.ident_1612 >= 'B') AND (a11.ident_1612 <= 'H')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'E') AND (a11.ident_1612 <= 'P')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'H') AND (a11.ident_1612 <= 'U')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'U') AND (a11.ident_1612 <= 'e')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'U') AND (a11.ident_1612 <= 'e')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'U') AND (a11.ident_1612 <= 'e')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))) OR ((((a11.ident_1612 >= 'P') AND (a11.ident_1612 <= 'e')) AND ((a12.ident_2051 >= 6) AND (a12.ident_2051 <= 6))))))) AND ((a11.ident_2251 >= 258) AND (a11.ident_2251 <= 266)))) GROUP BY a12.ident_2051, a11.ident_1612 LIMIT 1000001";
			
			Parser p = new Parser("public", null, sql);
			Statement stmt = p.stmt;
			System.out.println(r.rewrite(stmt));
		}
	}
}
