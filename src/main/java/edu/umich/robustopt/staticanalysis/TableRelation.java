package edu.umich.robustopt.staticanalysis;

import java.util.HashMap;
import java.util.Map;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.Table;

import static edu.umich.robustopt.staticanalysis.SelectContext.toLowerCaseIfNecessary;
public class TableRelation implements ISqlRelation {
	
	private final Table table;
	
	public TableRelation(Table table) {
		this.table = table;
	}

	public Table getTable(){
		return table;
	}
	
	@Override
	public String toString() {
		return table.toString();
	}

	@Override
	public Map<String, Expression> getColumns(Map<String, Schema> schemas) {
		// XXX: assume no schema means "public" schema for now
		String schemaName = table.getSchemaName() != null ? toLowerCaseIfNecessary(table.getSchemaName()) : "public";
		Schema schema = schemas.get(schemaName);
		if (schema == null)
			throw new SemanticViolationException("No such schema: " + schemaName);
		SchemaTable st = schema.getTable(toLowerCaseIfNecessary(table.getName()));
		if (st == null)
			throw new SemanticViolationException("No such table: " + schemaName + "." + table.getName());
		Map<String, Expression> ret = new HashMap<String, Expression>();
		for (String cname : st.getColumns()) {
			Column col = new Column(table, cname);
			ret.put(toLowerCaseIfNecessary(cname), col);
		}
		return ret;
	}
}
