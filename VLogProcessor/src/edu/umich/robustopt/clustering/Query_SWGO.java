package edu.umich.robustopt.clustering;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;
import com.relationalcloud.tsqlparser.statement.Statement;

import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.staticanalysis.ColumnExtractor;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public class Query_SWGO extends Query {
	private Set<ColumnDescriptor> select = null;
	private Set<ColumnDescriptor> from = null;
	private Set<ColumnDescriptor> where = null;
	private List<ColumnDescriptor> group_by = null;
	private List<ColumnDescriptor> order_by = null;

	//TODO: We use collection, since sometime we pass in a List and sometimes a Set! We should break this into two constructors
	public Query_SWGO (Collection<ColumnDescriptor> selectColumns,
			Collection<ColumnDescriptor> fromColumns,
			Collection<ColumnDescriptor> whereColumns,
			List<ColumnDescriptor> groupByColumns,
			List<ColumnDescriptor> orderByColumns) throws CloneNotSupportedException {
		super(null);
		select = new HashSet<ColumnDescriptor>();
		from = new HashSet<ColumnDescriptor>();
		where = new HashSet<ColumnDescriptor>();
		group_by = new ArrayList<ColumnDescriptor>();
		order_by = new ArrayList<ColumnDescriptor>();

		for (ColumnDescriptor x : selectColumns)
			select.add(x.clone());
		
		for (ColumnDescriptor x : fromColumns)
			from.add(x.clone());
		
		for (ColumnDescriptor x : whereColumns)
			where.add(x.clone());
		
		for (ColumnDescriptor x : groupByColumns)
			group_by.add(x.clone());
		
		for (ColumnDescriptor x : orderByColumns)
			order_by.add(x.clone());
	}

	public Query_SWGO(String sql, Map<String, Schema> schemaMap) throws CloneNotSupportedException, ParseException {
		this((Integer)null, (Date)null, (Double)null, sql, schemaMap);
	}

	public Query_SWGO(Integer query_id, Date timestamp, Double latency, String sql, Map<String, Schema> schemaMap) throws CloneNotSupportedException, ParseException {
		super(query_id, timestamp, latency, sql);
		Parser p;
		//try {
			p = new Parser("public", null, sql);
			Statement stmt = p.stmt;
		 	ColumnExtractor ex = new ColumnExtractor(schemaMap);
			Query_SWGO q = ex.getColumnSummary(stmt);
			
			Set<ColumnDescriptor> selectColumns = q.getSelect();
			Set<ColumnDescriptor> fromColumns = q.getFrom();
			Set<ColumnDescriptor> whereColumns = q.getWhere();
			List<ColumnDescriptor> groupByColumns = q.getGroup_by();
			List<ColumnDescriptor> orderByColumns = q.getOrder_by();
			
			select = new HashSet<ColumnDescriptor>();
			from = new HashSet<ColumnDescriptor>();
			where = new HashSet<ColumnDescriptor>();
			group_by = new ArrayList<ColumnDescriptor>();
			order_by = new ArrayList<ColumnDescriptor>();

			for (ColumnDescriptor x : selectColumns)
				select.add(x.clone());
			
			for (ColumnDescriptor x : fromColumns)
				from.add(x.clone());
			
			for (ColumnDescriptor x : whereColumns)
				where.add(x.clone());
			
			for (ColumnDescriptor x : groupByColumns)
				group_by.add(x.clone());
			
			for (ColumnDescriptor x : orderByColumns)
				order_by.add(x.clone());
			
		/*} catch (ParseException e) {
			System.err.println("query " + sql + " couldn't be parsed!");
			e.printStackTrace();
		}*/
	}

	private static void Fill(Set<NamedIdentifier> buf, Collection<ColumnDescriptor> c) {
		for (ColumnDescriptor cd : c) 
			buf.add(cd.getTableFullName());
	}
	
	public Set<NamedIdentifier> extractTables() {
		Set<NamedIdentifier> tables = new HashSet<NamedIdentifier>();
		Fill(tables, select);
		Fill(tables, from);
		Fill(tables, where);
		Fill(tables, group_by);
		Fill(tables, order_by);
		return tables;
	}
	
		
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((from == null) ? 0 : from.hashCode());
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
		if (!(obj instanceof Query_SWGO))
			return false;
		Query_SWGO other = (Query_SWGO) obj;
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
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

	public Set<ColumnDescriptor> getSelect() {
		return Collections.unmodifiableSet(select);
	}

	public List<ColumnDescriptor> getGroup_by() {
		return Collections.unmodifiableList(group_by);
	}
		
	public List<ColumnDescriptor> getOrder_by() {
		return Collections.unmodifiableList(order_by);
	}
	
	public Set<ColumnDescriptor> getWhere() {
		return Collections.unmodifiableSet(where);
	}	
	
	public Set<ColumnDescriptor> getFrom() {
		return Collections.unmodifiableSet(from);
	}

	@Override
	public String toString() {
		return "SELECT "+select+ " FROM " + from + " WHERE " + where + " GROUP BY " + group_by + " ORDER BY " + order_by;
	}

	@Override
	public boolean isEmpty() {
		return select.isEmpty() && where.isEmpty() && group_by.isEmpty() && order_by.isEmpty();
	}
	
	public static class QParser extends QueryParser<Query_SWGO> {
		@Override
		public Query_SWGO parse(Integer query_id, Date timestamp, Double latency, String sql, Map<String, Schema> schemaMap)
				throws CloneNotSupportedException, ParseException {
			return new Query_SWGO(query_id, timestamp, latency, sql, schemaMap);
		}

		@Override
		public List<Query_SWGO> convertSqlListToQuery(List<String> sqlQueries, Map<String, Schema> schemaMap) throws Exception {
			List<Query_SWGO> queries = new ArrayList<Query_SWGO>();
			for (String sql : sqlQueries) {
				Query_SWGO query = new Query_SWGO(sql, schemaMap);
				queries.add(query);
			}
			return queries;
		}
	}
	
	public static void main(String args[]) throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();

		List<String> sqlQueries = new ArrayList<String>();
		sqlQueries.add("SELECT min(col18) FROM public.wide100 WHERE col18 <= 7998 AND col43 <= 1146 group by col3 order by col99, col10 LIMIT 10;");
		//sqlQueries.add("SELECT min(col18) FROM public.wide100 WHERE public.wide100.col18 = 7998 LIMIT 10;");
		System.out.println(new Query_SWGO.QParser().convertSqlListToQuery(sqlQueries , schemaMap));
	}
}
