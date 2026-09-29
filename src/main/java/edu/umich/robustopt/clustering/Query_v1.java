package edu.umich.robustopt.clustering;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query_v1 extends Query {
	private Set<String> select = null;
	private Set<String> group_by = null;
	private Set<String> order_by = null;
	private Set<String> where = null;
	
	public Query_v1() {
		select = new HashSet<String>();
		group_by = new HashSet<String>();
		order_by = new HashSet<String>();
		where = new HashSet<String>();
	}

	
	public Set<String> getSelect() {
		return select;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((group_by == null) ? 0 : group_by.hashCode());
		result = prime * result
				+ ((order_by == null) ? 0 : order_by.hashCode());
		result = prime * result + ((select == null) ? 0 : select.hashCode());
		result = prime * result + ((where == null) ? 0 : where.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Query_v1))
			return false;
		Query_v1 other = (Query_v1) obj;
		if (group_by == null) {
			if (other.group_by != null)
				return false;
		} else if (!group_by.equals(other.group_by))
			return false;
		if (order_by == null) {
			if (other.order_by != null)
				return false;
		} else if (!order_by.equals(other.order_by))
			return false;
		if (select == null) {
			if (other.select != null)
				return false;
		} else if (!select.equals(other.select))
			return false;
		if (where == null) {
			if (other.where != null)
				return false;
		} else if (!where.equals(other.where))
			return false;
		return true;
	}

	public void addSelect(String term) {
		select.add(term);
	}
	
	public Set<String> getGroup_by() {
		return group_by;
	}
	
	public void addGroup_by(String term) {
		group_by.add(term);
	}
	
	public Set<String> getOrder_by() {
		return order_by;
	}
	
	public void addOrder_by(String term) {
		order_by.add(term);	
	}

	public Set<String> getWhere() {
		return where;
	}
	
	public void addWhere(String term) {
		where.add(term);
	}
	
	@Override
	public String toString() {
		return "SELECT "+select+" WHERE " + where + " GROUP BY " + group_by + " ORDER BY " + order_by;
	}
	
}
