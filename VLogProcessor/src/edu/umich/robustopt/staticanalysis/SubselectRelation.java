package edu.umich.robustopt.staticanalysis;

import java.util.HashMap;
import java.util.Map;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.select.SelectBody;

import static edu.umich.robustopt.staticanalysis.SelectContext.toLowerCaseIfNecessary;
public class SubselectRelation implements ISqlRelation {

	private final String alias;
	private final SelectBody subselect;
	private final SelectContext subselectContext;

	public SubselectRelation(String alias, SelectBody subselect, SelectContext subselectContext) {
		this.alias = alias;
		this.subselect = subselect;
		this.subselectContext = subselectContext;
	}
	
	public SelectBody getSelect() {
		return subselect;
	}
	
	public SelectContext getSubselectContext() {
		return subselectContext;
	}
	
	@Override
	public String toString() {
		return subselect.toString();
	}

	@Override
	public Map<String, Expression> getColumns(Map<String, Schema> schemas) {
		Map<String, Expression> m = subselectContext.getProjections();
		Map<String, Expression> ret = new HashMap<String, Expression>();
		for (Map.Entry<String, Expression> e : m.entrySet()) {
			Column col = new Column(new Table(null, alias), e.getKey());
			ret.put(toLowerCaseIfNecessary(e.getKey()), col);
		}
		return ret;
	}
}
