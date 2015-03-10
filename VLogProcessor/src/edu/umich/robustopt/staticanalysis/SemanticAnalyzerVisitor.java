package edu.umich.robustopt.staticanalysis;

import static edu.umich.robustopt.staticanalysis.SelectContext.toLowerCaseIfNecessary;

import java.util.IdentityHashMap;
import java.util.Map;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.select.AllColumns;
import com.relationalcloud.tsqlparser.statement.select.AllTableColumns;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.SelectBody;
import com.relationalcloud.tsqlparser.statement.select.SelectExpressionItem;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;
import com.relationalcloud.tsqlparser.visitors.recursive.ASTContext;

public class SemanticAnalyzerVisitor extends DefaultSemanticallyAwareVisitor {
	
	private final IdentityHashMap<PlainSelect, SelectContext> selectContexts = 
			new IdentityHashMap<PlainSelect, SelectContext>();

	public SemanticAnalyzerVisitor(Map<String, Schema> schemas) {
		super(schemas);
	}

	public IdentityHashMap<PlainSelect, SelectContext> getSelectContexts() {
		return selectContexts;
	}
	
	@Override
	public void visitBegin(PlainSelect select) {
		SelectContext ctx = 
				selectContextStack.isEmpty() ? 
						new SelectContext() : new SelectContext(currentSelectContext());
		SelectContext ret = selectContexts.put(select, ctx);
		assert ret == null;
		selectContextStack.push(ctx);
	}
	
	@Override
	public void visitEnd(PlainSelect select) {
		SelectContext cur = currentSelectContext();
		assert cur == selectContexts.get(select);
		selectContextStack.pop();
	}
	
	@Override
	public void visitBegin(Table t) {
		if (currentASTContext() != ASTContext.FROM)
			return;
		String aliasName = t.getAlias() != null ? t.getAlias() : t.getName();
		currentSelectContext().addRelation(toLowerCaseIfNecessary(aliasName), new TableRelation(t));
	}
	
	@Override
	public void visitEnd(SubSelect ss) {
		if (currentASTContext() != ASTContext.FROM)
			return;
		SelectBody body = ss.getSelectBody();
		SelectContext ctx = selectContexts.get(body.getRepresentativePlainSelect());
		// this is OK because we run visitEnd() and not visitBegin()
		assert ctx != null;
		currentSelectContext().addRelation(
				toLowerCaseIfNecessary(ss.getAlias()), new SubselectRelation(ss.getAlias(), ss.getSelectBody(), ctx));
	}
	
	private static String projectionNameFor(Expression e) {
		// XXX: hacky- special case columns.
		//
		// This is because something like this is valid:
		// 
		// SELECT col FROM ( SELECT tbl.col FROM tbl ) AS n;
		//
		// But this isn't valid:
		// SELECT col + 1 FROM ( SELECT tbl.col + 1 FROM tbl ) AS n;
		//
		// so we can't use the fully qualified name as a projection name
		
		if (e instanceof Column) 
			return ((Column) e).getColumnName();
		
		return e.toString(); // XXX: we should create a hidden, unreferencable name
	}
	
	@Override
	public void visitBegin(SelectExpressionItem se) {
		assert currentASTContext() == ASTContext.PROJECTIONS;
		String aliasName = se.getAlias() != null ? 
				se.getAlias() : projectionNameFor(se.getExpression());
		currentSelectContext().addProjection(toLowerCaseIfNecessary(aliasName), se.getExpression());
	}
	
	@Override
	public void visitBegin(AllColumns ac) {
		assert currentASTContext() == ASTContext.PROJECTIONS;
		
		// for each relation, enumerate their columns
		for (Map.Entry<String, ISqlRelation> e : 
				currentSelectContext().getRelations().entrySet()) {
			Map<String, Expression> m = e.getValue().getColumns(schemas);
			for (Map.Entry<String, Expression> e0 : m.entrySet())
				currentSelectContext().addProjection(toLowerCaseIfNecessary(e0.getKey()), e0.getValue());
		}
	}
	
	@Override
	public void visitBegin(AllTableColumns tc) {
		assert currentASTContext() == ASTContext.PROJECTIONS;
		
		Table t = tc.getTable();
		if (t.getSchemaName() != null) {
			// XXX: kind of hacky- if there is a schema name, then it *must*
			// be a fully qualified name
			Schema s = schemas.get(toLowerCaseIfNecessary(t.getSchemaName()));
			if (s == null)
				throw new SemanticViolationException("No such schema: " + t.getSchemaName());
			SchemaTable st = s.getTable(toLowerCaseIfNecessary(t.getName()));
			if (st == null)
				throw new SemanticViolationException("No such table: " + t.getSchemaName() + "." + t.getName());
			for (String cname : st.getColumns()) {
				Column col = new Column(t, cname);
				currentSelectContext().addProjection(toLowerCaseIfNecessary(cname), col);
			}
		} else {
			// resolve via the SelectContext
			ISqlRelation rel = currentSelectContext().getRelations().get(toLowerCaseIfNecessary(t.getName()));
			if (rel == null)
				throw new SemanticViolationException("No such table: " + t.getName());
			Map<String, Expression> m = rel.getColumns(schemas);
			for (Map.Entry<String, Expression> e0 : m.entrySet())
				currentSelectContext().addProjection(toLowerCaseIfNecessary(e0.getKey()), e0.getValue());
		}
	}
}
