package edu.umich.robustopt.staticanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.expression.operators.relational.EqualsTo;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.GreaterThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThan;
import com.relationalcloud.tsqlparser.expression.operators.relational.MinorThanEquals;
import com.relationalcloud.tsqlparser.expression.operators.relational.NotEqualsTo;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;
import com.relationalcloud.tsqlparser.parser.TokenMgrError;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;

import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public class ForeignKeyInference {

	// treat as bi-directional (A=>B <=> B=>A)
	static class FKRelationship {
		public final ColumnDescriptor a;
		public final ColumnDescriptor b;
		public FKRelationship(ColumnDescriptor x, ColumnDescriptor y) {
			if (x.getQualifiedName().compareTo(y.getQualifiedName()) > 0) {
				// canonicalize ordering
				ColumnDescriptor tmp = x;
				x = y;
				y = tmp;
			}
			a = x;
			b = y;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((a == null) ? 0 : a.hashCode());
			result = prime * result + ((b == null) ? 0 : b.hashCode());
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			FKRelationship other = (FKRelationship) obj;
			if (a == null) {
				if (other.a != null)
					return false;
			} else if (!a.equals(other.a))
				return false;
			if (b == null) {
				if (other.b != null)
					return false;
			} else if (!b.equals(other.b))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "FKRelationship [a=" + a + ", b=" + b + "]";
		}
	}
	
	class FKVisitor extends DefaultSemanticallyAwareVisitor {
		private final Map<FKRelationship, Integer> relationships = new HashMap<FKRelationship, Integer>();
		private final IdentityHashMap<PlainSelect, SelectContext> selectContexts;
	
		public FKVisitor(IdentityHashMap<PlainSelect, SelectContext> selectContexts) {
			super(ForeignKeyInference.this.schemas);
			this.selectContexts = selectContexts;
		}
		
		public Map<FKRelationship, Integer> getRelationships() {
			return relationships;
		}
		
		@Override
		public void visitBegin(PlainSelect select) {
			SelectContext ctx = selectContexts.get(select);
			assert ctx != null;
			selectContextStack.push(ctx);
		}
		
		@Override
		public void visitEnd(PlainSelect select) {
			SelectContext cur = currentSelectContext();
			assert cur == selectContexts.get(select);
			selectContextStack.pop();
		}
		
		private ColumnDescriptor getSingleColumnDescriptor(Expression e) {
			if (!(e instanceof Column))
				return null;
			Column c = (Column) e;
			List<ColumnDescriptor> l = currentSelectContext().getColumnDescriptors(c, schemas);
			if (l.size() != 1)
				return null;
			return l.get(0);
		}
		
		private void procBinop(BinaryExpression be) {
			// XXX: only look @ equi-joins for now
			ColumnDescriptor l = getSingleColumnDescriptor(be.getLeftExpression());
			ColumnDescriptor r = getSingleColumnDescriptor(be.getRightExpression());
			if (l != null && r != null) {
				FKRelationship fk = new FKRelationship(l, r);
				Integer i = relationships.get(fk);
				if (i == null) {
					relationships.put(fk, 1);
				} else {
					relationships.put(fk, i + 1);
				}
			}
		}
		
		@Override
		public void visitEnd(EqualsTo eq) {
			procBinop(eq);
		}
		
		@Override
		public void visitEnd(NotEqualsTo neq) {
			procBinop(neq);
		}
		
		@Override
		public void visitEnd(GreaterThan gt) {
			procBinop(gt);
		}
		
		@Override
		public void visitEnd(GreaterThanEquals geq) {
			procBinop(geq);
		}

		@Override
		public void visitEnd(MinorThan lt) {
			procBinop(lt);
		}
		
		@Override
		public void visitEnd(MinorThanEquals leq) {
			procBinop(leq);
		}
		
	}
	
	private final Map<String, Schema> schemas;
	
	public ForeignKeyInference(Map<String, Schema> schemas) {
		this.schemas = schemas;
	}
	
	public Map<FKRelationship, Integer> getRelationships(Statement stmt) {
		SemanticAnalyzerVisitor semanticAnalyzer = new SemanticAnalyzerVisitor(schemas);
		stmt.accept(semanticAnalyzer);
		IdentityHashMap<PlainSelect, SelectContext> contexts = semanticAnalyzer.getSelectContexts();
		FKVisitor v = new FKVisitor(contexts);
		stmt.accept(v);
		return v.getRelationships();
	}
	
	private static Map<FKRelationship, Integer> MergeFKMaps(
			Map<FKRelationship, Integer> a, 
			Map<FKRelationship, Integer> b) {
		 Map<FKRelationship, Integer> res = new HashMap<FKRelationship, Integer>();
		
		 for (Map.Entry<FKRelationship, Integer> e : a.entrySet()) {
			 Integer i = res.get(e.getKey());
			 if (i == null) {
				 res.put(e.getKey(), e.getValue());
			 } else {
				 res.put(e.getKey(), i + e.getValue());
			 }
		 }
		 
		 for (Map.Entry<FKRelationship, Integer> e : b.entrySet()) {
			 Integer i = res.get(e.getKey());
			 if (i == null) {
				 res.put(e.getKey(), e.getValue());
			 } else {
				 res.put(e.getKey(), i + e.getValue());
			 }
		 }
		 
		 return res;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SchemaDescriptor desc = SchemaUtils.GetSchemaMapFromDefaultSources("example_dataset", VerticaDatabaseLoginConfiguration.class.getSimpleName());
		ForeignKeyInference fki = new ForeignKeyInference(desc.getSchemas());
		Map<FKRelationship, Integer> relationships = new HashMap<FKRelationship, Integer>();
		boolean DoFile = true;
		if (DoFile) {
			File f = new File("src/test/resources/MinerTest/sample_query.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = null;
			int linenum = 1;
			while ((line = br.readLine()) != null) {
				int idx0 = line.indexOf('|');
				assert idx0 != -1;
				String line1 = line.substring(idx0 + 1);
				int idx1 = line1.indexOf('|');
				assert idx1 != -1;
				String query = line1.substring(idx1 + 1);
			
				try {
					Parser p = new Parser("public", null, query);
					Statement stmt = p.stmt;
					if (stmt instanceof Select) {
						relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
					}
				} catch (ParseException e) {
					//System.out.println("[PARSE] Error on line: " + linenum + ". Query: " + query);
				} catch (TokenMgrError e) {
					System.out.println("[PARSE/TOKEN] Error on line: " + linenum + ". Query: " + query);
				} catch (Throwable t) {
					System.out.println("[INFERENCE] Error on line: " + linenum + ". Query: " + query);
					t.printStackTrace();
				}
				
				linenum++;
			}
			br.close();
			
		} else {
			//Parser p = new Parser("public", null, "select  sum(case when a11.example_column='k56' then 1 else 0 end) AS example_column,   sum(a11.example_column) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then age_in_months(a11.example_column,a11.example_column) * a11.example_column end) AS WJXBFS1,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column) else null end) AS WJXBFS2,  sum(a11.example_column) AS example_column,   sum(case when a11.example_column='e/M' then a11.example_column else 0 end) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then  a11.example_column end) AS WJXBFS3,   sum(case when a11.example_column in ('EsE','HqM','Uew','e/M') then a11.example_column else null end) AS example_column,   sum(a11.example_column) AS CURRINVBAL1MAGO,  sum(Case when a11.example_column in ('EsE','HqM','Uew','e/M') and a11.example_column=a11.example_column then a11.example_column end) AS PAYOFFBALANCECDR,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column * a11.example_column) else  null end) AS WJXBFS4,   sum(case when a11.example_column is not null and a11.example_column in (6,14) then Case when a11.example_column=a11.example_column then a11.example_column end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.example_column='Uew' then a11.example_column else 0 end) AS example_column from example_schema.example_column  a11 where a11.example_column in (278, 0) LIMIT 1000001");
			//Parser p = new Parser("public", null, "select * from example_schema.example_column a11 where a11.example_column='k56'");
			//Parser p = new Parser("public", null, "SELECT a12.example_column AS example_column, max(a12.example_column) AS example_column, a11.example_column AS example_column, a11.example_column AS example_column, sum(a11.example_column) AS example_column FROM example_schema.example_column AS a11 JOIN example_schema.example_column AS a12 ON ((a11.example_column = a12.example_column)) WHERE ((((a11.example_column IN (277) AND a12.example_column IN (1, 2, 3, 4, 5, 6)) AND (a11.example_column > 2000)) AND a11.example_column BETWEEN 721 AND 740)) GROUP BY a12.example_column, a11.example_column, a11.example_column LIMIT 1000001");
			
			Parser p = new Parser("public", null, "select a.example_column from (select distinct example_column as example_column from example_schema.example_column where example_column > 0) a where not exists (select example_column from example_schema.example_column b where b.example_column = 'nUk93d' and a.example_column = b.example_column)");
			Statement stmt = p.stmt;
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
			relationships = MergeFKMaps(relationships, new HashMap<FKRelationship, Integer>());
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
			relationships = MergeFKMaps(relationships, new HashMap<FKRelationship, Integer>());
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
		}
		
		//System.out.println(relationships);
	
		File of = new File("target/example-fk-relationships.txt");
		PrintWriter pw = new PrintWriter(of);
		for (Map.Entry<FKRelationship, Integer> e : relationships.entrySet()) {
			pw.println(e.getKey().a.getQualifiedName() + " " + e.getKey().b.getQualifiedName() + " " + e.getValue());
		}
		pw.close();
	}

}
