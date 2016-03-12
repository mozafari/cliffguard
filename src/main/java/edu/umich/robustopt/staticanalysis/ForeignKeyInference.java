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
		SchemaDescriptor desc = SchemaUtils.GetSchemaMapFromDefaultSources("dataset1", VerticaDatabaseLoginConfiguration.class.getSimpleName());
		ForeignKeyInference fki = new ForeignKeyInference(desc.getSchemas());
		Map<FKRelationship, Integer> relationships = new HashMap<FKRelationship, Integer>();
		boolean DoFile = true;
		if (DoFile) {
			File f = new File(GlobalConfigurations.RO_BASE_PATH, "VLogProcessor/data/both-considered");
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
			//Parser p = new Parser("public", null, "select  sum(case when a11.ident_2629='k56' then 1 else 0 end) AS ident_199,   sum(a11.ident_2090) AS ident_2272,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then age_in_months(a11.ident_421,a11.ident_225) * a11.ident_305 end) AS WJXBFS1,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_305) else null end) AS WJXBFS2,  sum(a11.ident_2386) AS ident_445,   sum(case when a11.ident_1980='e/M' then a11.ident_2090 else 0 end) AS ident_386,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then  a11.ident_305 end) AS WJXBFS3,   sum(case when a11.ident_1980 in ('EsE','HqM','Uew','e/M') then a11.ident_2090 else null end) AS ident_1869,   sum(a11.ident_552) AS CURRINVBAL1MAGO,  sum(Case when a11.ident_1612 in ('EsE','HqM','Uew','e/M') and a11.ident_2251=a11.ident_1817 then a11.ident_2471 end) AS PAYOFFBALANCECDR,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_341 * a11.ident_305) else  null end) AS WJXBFS4,   sum(case when a11.ident_2386 is not null and a11.ident_1779 in (6,14) then Case when a11.ident_2251=a11.ident_1817 then a11.ident_552 end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.ident_1980='Uew' then a11.ident_2090 else 0 end) AS ident_1637 from st_etl_2.ident_164  a11 where a11.ident_2251 in (278, 0) LIMIT 1000001");
			//Parser p = new Parser("public", null, "select * from st_etl_2.ident_164 a11 where a11.ident_2629='k56'");
			//Parser p = new Parser("public", null, "SELECT a12.ident_2051 AS ident_1019, max(a12.ident_2071) AS ident_2071, a11.ident_1351 AS ident_1351, a11.ident_471 AS ident_471, sum(a11.ident_2090) AS ident_2272 FROM st_etl_2.ident_164 AS a11 JOIN st_etl_2.ident_29 AS a12 ON ((a11.ident_1980 = a12.ident_1980)) WHERE ((((a11.ident_2251 IN (277) AND a12.ident_2051 IN (1, 2, 3, 4, 5, 6)) AND (a11.ident_1351 > 2000)) AND a11.ident_2224 BETWEEN 721 AND 740)) GROUP BY a12.ident_2051, a11.ident_1351, a11.ident_471 LIMIT 1000001");
			
			Parser p = new Parser("public", null, "select a.ident_2243 from (select distinct ident_378 as ident_2243 from st_etl_2.ident_85 where ident_378 > 0) a where not exists (select ident_2251 from st_etl_2.ident_164 b where b.ident_1187 = 'nUk93d' and a.ident_2243 = b.ident_2251)");
			Statement stmt = p.stmt;
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
			relationships = MergeFKMaps(relationships, new HashMap<FKRelationship, Integer>());
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
			relationships = MergeFKMaps(relationships, new HashMap<FKRelationship, Integer>());
			relationships = MergeFKMaps(relationships, fki.getRelationships(stmt));
		}
		
		//System.out.println(relationships);
	
		File of = new File(GlobalConfigurations.RO_BASE_PATH, "VLogProcessor/data/both-considered-fk-relationships");
		PrintWriter pw = new PrintWriter(of);
		for (Map.Entry<FKRelationship, Integer> e : relationships.entrySet()) {
			pw.println(e.getKey().a.getQualifiedName() + " " + e.getKey().b.getQualifiedName() + " " + e.getValue());
		}
		pw.close();
	}

}
