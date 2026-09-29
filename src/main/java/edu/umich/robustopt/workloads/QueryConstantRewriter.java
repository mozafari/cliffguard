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
			File f = new File("src/test/resources/MinerTest/sample_query.txt");
			File of = new File("target/example-query-rewrite.txt");
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
			//Parser p = new Parser("public", null, "select  sum(case when a11.example_column='k56' then 1 else 0 end) AS example_column,   sum(a11.example_column) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then age_in_months(a11.example_column,a11.example_column) * a11.example_column end) AS WJXBFS1,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column) else null end) AS WJXBFS2,  sum(a11.example_column) AS example_column,   sum(case when a11.example_column='e/M' then a11.example_column else 0 end) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then  a11.example_column end) AS WJXBFS3,   sum(case when a11.example_column in ('EsE','HqM','Uew','e/M') then a11.example_column else null end) AS example_column,   sum(a11.example_column) AS CURRINVBAL1MAGO,  sum(Case when a11.example_column in ('EsE','HqM','Uew','e/M') and a11.example_column=a11.example_column then a11.example_column end) AS PAYOFFBALANCECDR,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column * a11.example_column) else  null end) AS WJXBFS4,   sum(case when a11.example_column is not null and a11.example_column in (6,14) then Case when a11.example_column=a11.example_column then a11.example_column end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.example_column='Uew' then a11.example_column else 0 end) AS example_column from example_schema.example_column  a11 where a11.example_column in (278, 0) LIMIT 1000001");
			//Parser p = new Parser("public", null, "select * from example_schema.example_column a11 where a11.example_column='k56'");
			//Parser p = new Parser("public", null, "SELECT a12.example_column AS example_column, max(a12.example_column) AS example_column, a11.example_column AS example_column, a11.example_column AS example_column, sum(a11.example_column) AS example_column FROM example_schema.example_column AS a11 JOIN example_schema.example_column AS a12 ON ((a11.example_column = a12.example_column)) WHERE ((((a11.example_column IN (277) AND a12.example_column IN (1, 2, 3, 4, 5, 6)) AND (a11.example_column > 2000)) AND a11.example_column BETWEEN 721 AND 740)) GROUP BY a12.example_column, a11.example_column, a11.example_column LIMIT 1000001");
			
			//String sql = "select a.example_column, case when a.example_column > 0 then '79c' else '+nU' end as With_Loss_Amount,  case when a.example_column = 'HqM' then '79c' else '+nU' end as ShortSale,  count(*) from example_schema.example_column a join example_schema.example_column b on a.example_column = b.example_column and a.example_column = b.example_column +1 where a.example_column = '+nU' and b.example_column in ('PJs','Bf6') group by a.example_column,  case when a.example_column > 0 then '79c' else '+nU' end, case when a.example_column = 'HqM' then '79c' else '+nU' end order by a.example_column";
			String sql = "SELECT a12.example_column AS example_column, a11.example_column AS example_column, sum(CASE WHEN (a11.example_column > 0) THEN a11.example_column ELSE a11.example_column END) AS LASTBALANCE FROM example_schema.example_column AS a11 JOIN example_schema.example_column AS a12 ON ((a11.example_column = a12.example_column)) WHERE (((((((((a11.example_column) IN (SELECT c21.example_column FROM example_schema.example_column AS c21 WHERE ((c21.example_column >= 'G4649512c   ') AND (c21.example_column <= 'G4h70W708LoE')))) AND (a11.example_column >= 120)) AND (a11.example_column < 60)) AND (a11.example_column > 180)) AND (a11.example_column <= 120)) AND ((((((((((((a11.example_column >= '+') AND (a11.example_column <= 'E')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6)))) OR ((((a11.example_column >= 'B') AND (a11.example_column <= 'H')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'E') AND (a11.example_column <= 'P')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'H') AND (a11.example_column <= 'U')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'U') AND (a11.example_column <= 'e')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'U') AND (a11.example_column <= 'e')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'U') AND (a11.example_column <= 'e')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))) OR ((((a11.example_column >= 'P') AND (a11.example_column <= 'e')) AND ((a12.example_column >= 6) AND (a12.example_column <= 6))))))) AND ((a11.example_column >= 258) AND (a11.example_column <= 266)))) GROUP BY a12.example_column, a11.example_column LIMIT 1000001";
			
			Parser p = new Parser("public", null, sql);
			Statement stmt = p.stmt;
			System.out.println(r.rewrite(stmt));
		}
	}
}
