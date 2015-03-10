package edu.umich.robustopt.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import java.util.Date;

import com.relationalcloud.tsqlparser.loader.Schema;

/*
 * A Query can be both a single query or a cluster of similar queries. 
 * This can be tested by calling isQuery() member
 */
public abstract class Query implements Serializable, Cloneable {
	private static final long serialVersionUID = -4564760606200765679L;
	private Integer query_id = null;
	private Double originalLatency = null;
	private String sql = null;	
	private Date timestamp = null;
	
	public Query(String sql) {
		this(null, null, null, sql);
	}
	
	public Query(Integer query_id, Date timestamp, Double latency, String sql) {
		this.query_id = query_id;
		this.timestamp = timestamp;
		this.originalLatency = latency;
		this.sql = sql;
	}
	
	@Override
	public Query clone() throws CloneNotSupportedException {
		Query q;
		q = (Query) super.clone();	
		q.query_id = this.query_id;
		q.originalLatency = this.originalLatency;
		q.sql = this.sql;
		Date d = this.timestamp;
		q.timestamp = (d!=null ? (Date)d.clone() : null); 
		
		return q;
	}
 		
	public Integer getQuery_id() throws Exception {
		return query_id;
	}

	public Double getOriginalLatency() throws Exception {
		return originalLatency;
	}

	public String getSql() throws Exception {
		if (sql == null) {
			throw new Exception("You have never assigned the sql: " + (Query_SWGO)this);
		}
		return sql;
	}

	public Date getTimestamp() throws Exception {
		return timestamp;
	}
	
	public abstract boolean isEmpty();
	
	public static List<Query> convertToListOfQuery(List<? extends Query> queries) throws CloneNotSupportedException {
		List<Query> results = new ArrayList<Query>();
		for (int q=0; q<queries.size(); ++q) 
			results.add(queries.get(q).clone());
		return results;
	}
	
	/*
	public static List<List<Query>> convertToListOfListOfQuery(List<List<? extends Query>> queries) throws CloneNotSupportedException {
		List<List<Query>> results = new ArrayList<List<Query>>();
		for (int w=0; w<queries.get(w).size(); ++w) {
			results.add(new ArrayList<Query>());
			for (int q=0; q<queries.get(w).size(); ++q) 
				results.get(w).add(queries.get(w).get(q).clone());
		}
		return results;
	}
	*/
}
