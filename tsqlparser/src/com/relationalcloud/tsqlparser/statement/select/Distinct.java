package com.relationalcloud.tsqlparser.statement.select;

import java.util.List;

import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;

/**
 * A DISTINCT [ON (expression, ...)] clause
 */
public class Distinct {
	private List onSelectItems;
	
	/**
	 * A list of {@link SelectItem}s expressions, as in "select DISTINCT ON (a,b,c) a,b FROM..." 
	 * @return a list of {@link SelectItem}s expressions
	 */
	public List getOnSelectItems() {
		return onSelectItems;
	}

	public void setOnSelectItems(List list) {
		onSelectItems = list;
	}

	public String toString() {
		String sql = "DISTINCT";
		
		if(onSelectItems != null && onSelectItems.size() > 0) {
			sql += " ON ("+PlainSelect.getStringList(onSelectItems)+")";
		}
		
		return sql;
	}
	
	public void accept(RecursiveVisitor v) {
		v.visitBegin(this);
		List<SelectItem> l = getOnSelectItems();
		if (l != null)
			for (SelectItem si : l)
				si.accept(v);
		v.visitEnd(this);
	}
	
	public Object accept(RecursiveRewriterVisitor v) {
		v.visitBegin(this);
		List<SelectItem> l = getOnSelectItems();
		if (l != null)
			for (int i = 0; i < l.size(); i++) {
				Object s = l.get(i).accept(v);
				if (s != null)
					l.set(i, (SelectItem) s);
			}
		return v.visitEnd(this);
	}
}
