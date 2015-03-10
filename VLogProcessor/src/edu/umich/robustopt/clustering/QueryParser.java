package edu.umich.robustopt.clustering;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.parser.ParseException;

import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public abstract class QueryParser<Q extends Query> {
	public abstract Q parse(Integer query_id, Date timestamp, Double latency, String sql, Map<String, Schema> schemaMap) throws CloneNotSupportedException, ParseException;

	public Q parse(String sql, Map<String, Schema> schemaMap) throws CloneNotSupportedException, ParseException {
		return parse ((Integer)null, (Date)null, (Double)null, sql, schemaMap);
	}
	
	public abstract List<Q> convertSqlListToQuery(List<String> sqlQueries, Map<String, Schema> schemaMap) throws Exception;
	
	public List<List<Q>> convertSqlListOfListToQuery(List<List<String>> sqlQueriesList, Map<String, Schema> schemaMap) throws Exception {
		List<List<Q>> queryListList = new ArrayList<List<Q>>();
		for (List<String> sqlQueries : sqlQueriesList)
			queryListList.add(convertSqlListToQuery(sqlQueries, schemaMap));
		return queryListList;			
	}

	public static void main(String[] args) {
		String sql = "select count(*) from st_etl_2.ident_164 where ident_2251 = 'BO2'";
		QueryParser<Query_SWGO> qParser = new Query_SWGO.QParser();
		Map<String, Schema> schemaMap;
		try {
			schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		Query_SWGO query;
		try {
			query = qParser.parse(sql, schemaMap);
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		System.out.println("This query should not have been parsed because it's not valid according to the schema, i.e. its column type is integer not varchar (Look at line 7967 of dataset19_schema_def):\n " + sql);
	}
	
}
