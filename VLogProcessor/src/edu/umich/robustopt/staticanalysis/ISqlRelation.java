package edu.umich.robustopt.staticanalysis;

import java.util.Map;

import com.relationalcloud.tsqlparser.expression.Expression;
import com.relationalcloud.tsqlparser.loader.Schema;

public interface ISqlRelation {
	public Map<String, Expression> getColumns(Map<String, Schema> schemas);
}
