package edu.umich.robustopt.staticanalysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.ASTContext;
import com.relationalcloud.tsqlparser.visitors.recursive.DefaultRecursiveVisitor;

import edu.umich.robustopt.clustering.Query_SWGO;

public class ColumnExtractor {
	
	private final Map<String, Schema> schemas;
	
	private final Set<ColumnDescriptor> select = new HashSet<ColumnDescriptor>();
	private final Set<ColumnDescriptor> from = new HashSet<ColumnDescriptor>();
	private final Set<ColumnDescriptor> where = new HashSet<ColumnDescriptor>();
	private final List<ColumnDescriptor> group_by = new ArrayList<ColumnDescriptor>();
	private final List<ColumnDescriptor> order_by = new ArrayList<ColumnDescriptor>();
	
	public ColumnExtractor(Map<String, Schema> schemas) {
		this.schemas = schemas;
	}
	
	private class ExtractorVisitor extends DefaultSemanticallyAwareVisitor {
		
		private final IdentityHashMap<PlainSelect, SelectContext> selectContexts;
		
		//private final Query_SWGO summary = new Query_SWGO();
		
		public ExtractorVisitor(IdentityHashMap<PlainSelect, SelectContext> selectContexts) {
			super(ColumnExtractor.this.schemas);
			this.selectContexts = selectContexts;
		}
		
		public Query_SWGO getSummary() throws CloneNotSupportedException {
			return new Query_SWGO(select, from, where, group_by, order_by);
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
		
		@Override
		public void visitBegin(Column c) {
			Collection<ColumnDescriptor> buffer = null;
			switch (astContextStack.get(0)) {
			case PROJECTIONS:
				buffer = select;
				break;
			case WHERE:
				buffer = where;
				break;
			case GROUP_BY:
				buffer = group_by;
				break;
			case ORDER_BY:
				buffer = order_by;
				break;
			default:
				// XXX: handle
				assert false;
			}
			assert buffer != null;
			buffer.addAll(currentSelectContext().getColumnDescriptors(c, schemas));
		}
		
	}
	
	public Query_SWGO getColumnSummary(Statement stmt) throws CloneNotSupportedException {
		
		// run semantic analysis
		SemanticAnalyzerVisitor semanticAnalyzer = new SemanticAnalyzerVisitor(schemas);
		stmt.accept(semanticAnalyzer);
		IdentityHashMap<PlainSelect, SelectContext> contexts = semanticAnalyzer.getSelectContexts();

		ExtractorVisitor extractor = new ExtractorVisitor(contexts);
		stmt.accept(extractor);

		return extractor.getSummary();
	}
	
}
