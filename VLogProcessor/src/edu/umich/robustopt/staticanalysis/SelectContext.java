package edu.umich.robustopt.staticanalysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.DefaultRecursiveVisitor;

import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.clustering.UnpartitionedQueryLogAnalyzer;
import edu.umich.robustopt.clustering.Query_SWGO.QParser;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;
import edu.umich.robustopt.workloads.DistributionDistancePair;

/**
 * Should be one associated with every {@link PlainSelect}
 * @author stephentu
 *
 */
public class SelectContext {
	
	public static final boolean NameResolutionLowerCase = false;
	
	public static String toLowerCaseIfNecessary(String s) {
		if (NameResolutionLowerCase)
			return s.toLowerCase();
		return s;
	}
	
	private final SelectContext parent;

	private final Map<String, Expression> projections = 
			new HashMap<String, Expression>();
	
	private final Map<String, ISqlRelation> relations = 
			new HashMap<String, ISqlRelation>();
	
	public SelectContext() {
		this.parent = null;
	}
	
	public SelectContext(SelectContext parent) {
		this.parent = parent;
	}
	
	public Map<String, Expression> getProjections() {
		return projections;
	}
	
	public Map<String, ISqlRelation> getRelations() {
		return relations;
	}
	
	public void addProjection(String name, Expression expr) {
		projections.put(name, expr);
	}
	
	public void addRelation(String name, ISqlRelation relation) {
		relations.put(name, relation);
	}
	
	private static class GreedyColumnExtractorVisitor extends DefaultRecursiveVisitor {
		private List<Column> columns = new ArrayList<Column>();
		public List<Column> getColumns() {
			return columns;
		}
		public void visitBegin(Column c) {
			columns.add(c);
		}
		
		public static List<Column> getColumnsFor(Expression e) {
			GreedyColumnExtractorVisitor v = new GreedyColumnExtractorVisitor();
			e.accept(v);
			return v.getColumns();
		}
	};
	
	private static List<ColumnDescriptor> tryResolveInRelation(
			Column c, ISqlRelation reln, Map<String, Schema> schemas) {
		// XXX: hacky
		if (reln instanceof TableRelation) {
			TableRelation tr = (TableRelation) reln;
			String schemaName = tr.getTable().getSchemaName() != null ? toLowerCaseIfNecessary(tr.getTable().getSchemaName()) : "public";
			Schema schema = schemas.get(schemaName);
			assert schema != null;
			SchemaTable st = schema.getTable(toLowerCaseIfNecessary(tr.getTable().getName()));
			assert st != null;
			if (st.getColumnByName(toLowerCaseIfNecessary(c.getColumnName())) != null)
				return Collections.singletonList(
						new ColumnDescriptor(
							schemaName, 
							tr.getTable().getName(), 
							c.getColumnName()));
			return null;
		} else if (reln instanceof SubselectRelation) {
			SubselectRelation sr = (SubselectRelation) reln;
			SelectContext subCtx = sr.getSubselectContext();
			Expression e = subCtx.getProjections().get(toLowerCaseIfNecessary(c.getColumnName()));
			if (e == null)
				return null;
			List<Column> cols = GreedyColumnExtractorVisitor.getColumnsFor(e);
			List<ColumnDescriptor> ret = new ArrayList<ColumnDescriptor>();
			for (Column col : cols)
				ret.addAll(subCtx.getColumnDescriptors(col, schemas));
			return ret;
		} else 
			// XXX: unhandled
			assert false;
		return null;
	}
	
	public List<ColumnDescriptor> getColumnDescriptors(Column c, Map<String, Schema> schemas) {
		if (c.getTable().getSchemaName() != null) {
			// easy case
			// XXX: assert valid fully qualified name
			return Collections.singletonList(
				new ColumnDescriptor(
					c.getTable().getSchemaName(), 
					c.getTable().getName(), 
					c.getColumnName()));
		} else {
			// hard case- need to recursively resolve using the SelectContext
			if (c.getTable().getName() != null) {
				ISqlRelation reln = getRelations().get(toLowerCaseIfNecessary(c.getTable().getName()));
				if (reln == null) {
					if (parent == null)
						throw new SemanticViolationException(
							"Cannot find table named: " + toLowerCaseIfNecessary(c.getTable().getName()));
					else
						return parent.getColumnDescriptors(c, schemas);
				}
				List<ColumnDescriptor> ret = tryResolveInRelation(c, reln, schemas);
				if (ret == null || ret.isEmpty())
					throw new SemanticViolationException(
						"Cannot find column " + toLowerCaseIfNecessary(c.getColumnName()) + " in table " + toLowerCaseIfNecessary(c.getTable().toString()));
				return ret;
			} else {
				// need to do a full search over all the schemas
				for (Map.Entry<String, ISqlRelation> e : getRelations().entrySet()) {
					List<ColumnDescriptor> ret = tryResolveInRelation(c, e.getValue(), schemas);
					if (ret != null)
						// found
						return ret;
				}
				if (parent != null)
					return parent.getColumnDescriptors(c, schemas);
			}
		}
		throw new SemanticViolationException("Unresolved Column: " + toLowerCaseIfNecessary(c.toString()));
	}
	
	@Override
	public String toString() {
		return "[projections=" + projections + ", relations=" + relations + "]";
	}
	
	public static void main(String args[]) {
		try {
			String dbAlias = "dataset1";
			SchemaDescriptor schemaDesc = SchemaUtils.GetSchemaMapFromDefaultSources(dbAlias, VerticaDatabaseLoginConfiguration.class.getSimpleName());
			QParser qParser = new Query_SWGO.QParser();
			
			String sql = "SELECT ident_205,ident_365 FROM ident_59 ORDER BY ident_365";
			Query_SWGO query = qParser.parse(sql, schemaDesc.getSchemas());
			
			System.out.println("Query parsed successfully: "+ query.toString());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
