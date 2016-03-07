package edu.umich.robustopt.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryWindow implements Cloneable {
	private List<Query> queries;
	
	public QueryWindow(List<? extends Query> queries) throws CloneNotSupportedException {
		this.queries = new ArrayList<Query>();
		for (int i=0; i<queries.size(); ++i)
			this.queries.add(queries.get(i).clone());
	}
	
	@Override
	public String toString() {
		return "Window [queries=" + queries + "]";
	}
	
	public List<Query> getQueries() throws CloneNotSupportedException {
		return Collections.unmodifiableList(queries);
	}
	
	@Override
	public QueryWindow clone() throws CloneNotSupportedException {
		List<Query> newQueries = new ArrayList<Query>();
		
		for (int i=0; i<queries.size(); ++i)
			newQueries.add(queries.get(i).clone());
			
		QueryWindow copy = new QueryWindow(newQueries);
		
		return copy;
	}
	
}
