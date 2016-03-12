package edu.umich.robustopt.clustering;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import edu.umich.robustopt.clustering.Query_v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import au.com.bytecode.opencsv.CSVReader;


public class QueryClusteringVerticaColumns extends QueryClustering {

	private List<String> header = null;
	private List<Map<String, String>> rows = null;
	
	/**
	 * @param args
	 */
	
	public void loadVExportedCsv(String filename) {
	    CsvListReader reader;
		try {
			CsvListReader csvReader = new CsvListReader(new FileReader(filename), CsvPreference.STANDARD_PREFERENCE);		    	    
			//Read CSV Header
			header = new ArrayList<String>(csvReader.read());
			List<String> rowAsTokens;

			//Read the CSV as List of Maps where each Map represents row data
			rows = new ArrayList<Map<String, String>>();
			Map<String, String> row = null;
			  
			while ((rowAsTokens = csvReader.read()) != null) {
				//Create Map for each row in CSV
				row = new HashMap<String, String>();				
				for (int i = 0 ; i < header.size() ; i ++) {
					row.put(header.get(i), rowAsTokens.get(i));
				}
			
				//add Row map to list of rows
				rows.add(row);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	public static void main(String[] args) {		
		
		String csvFile = "../DBD-parser/barzan_good_join.csv";
		QueryClusteringVerticaColumns vFeaturesClustering = new QueryClusteringVerticaColumns();
		vFeaturesClustering.loadVExportedCsv(csvFile);
		vFeaturesClustering.showHeader();
		//vFeaturesClustering.showDomains(20);
		Map<Integer, Query_v1> queries = vFeaturesClustering.summarizeQueries();
		System.out.println("We had " + queries.size() + " queries.");
		List<Integer> queryIds = new ArrayList<Integer>();
		for (int queryId : queries.keySet()) {
			queryIds.add(queryId);
		}
		
		Collections.sort(queryIds);
		
		int step = queryIds.size() / 5;
		List<Set<Query_v1>> clusters = new ArrayList<Set<Query_v1>>();
		Set<Query_v1> prevCluster = null;
		Set<Query_v1> allClusters = vFeaturesClustering.collapse(queries, queryIds.get(0), queryIds.get(queryIds.size()-1));
		System.out.println("We had "+allClusters.size()+" clusters");
		for (int i=0; i<queryIds.size(); i=i+step) {
			int last = (i+step >= queryIds.size() ? queryIds.size()-1 : i+step);
			Set<Query_v1> cluster = vFeaturesClustering.collapse(queries, queryIds.get(i), queryIds.get(last));
			clusters.add(cluster);
			if (prevCluster!=null) {
				Set<Query_v1> commonCluster = new HashSet<Query_v1>(prevCluster);
				prevCluster.removeAll(cluster); //unique in the old set
				commonCluster.retainAll(cluster); //common
				int uniqueInOld = prevCluster.size();
				int commonOnes = commonCluster.size();
				int uniqueInNew = cluster.size() - commonOnes;
				System.out.println("uniqueOld: "+ uniqueInOld + " common: " + commonOnes + " uniqueNew: " + uniqueInNew);
				for (Query_v1 q:commonCluster) {
					System.out.println("# " + q);
				}
			}
			prevCluster = cluster;
		}
		
		
	}

	private Set<Query_v1> collapse(Map<Integer, Query_v1> queries, int lower, int upper) {
		Set<Query_v1> cluster = new HashSet<Query_v1>();
		for (int key : queries.keySet()) {
			if (key < lower || key > upper) {
				continue;
			}
			Query_v1 query = queries.get(key);
			if (!cluster.contains(query)) {
				cluster.add(query);
			}
		}
		
		return cluster;
	}


	public void showRows() {
		//Iterate
		for (Map<String, String> rowMap : rows) {
			System.out.println(rowMap);
		}		    	  
	}
	
	public void showHeader() {
		for (String key: header) {
			System.out.println(key);
		}
	}
	
	public void showDomains(int maxCardinality) {
		for (String key: header) {
			Map<String, Integer> uniqueValues = new HashMap<String, Integer>();
			for (Map<String, String> rowMap : rows) {
				String value = rowMap.get(key);
				if (!uniqueValues.containsKey(value)) {
					uniqueValues.put(value, 1);
				} else {
					uniqueValues.put(value, uniqueValues.get(value)+1);
				}
			}
			if (uniqueValues.size()<maxCardinality) {
				System.out.println("Domain of " + key + " = " + uniqueValues);
			}
		}
		/*
		design_query_id
		expression_string
		clause
		expression_type
		alias
		table_schema
		table_name
		query_text
		*/
		/*expression_type: 
		 JOIN_OTHER
		 PRED_1_COLUMN
		 TABLE_COLUMN
		 JOIN_SIMPLE_EQ
		 OTHER_EXPR
		 AGGREGATE
		 PRED_OTHER
		(7 rows)

		dbadmin=> select distinct clause from barzan_good_join;
		       clause       
		--------------------
		 DURING_JOIN_FILTER
		 POST_JOIN_FILTER
		 ORDER_BY
		 GROUP_BY
		 JOIN
		 SELECT
		 PRE_JOIN_FILTER
		*/		
	}
	public Map<Integer, Query_v1> summarizeQueries() {
		Map<Integer, Query_v1> queries = new HashMap<Integer, Query_v1>();
		for (Map<String, String> rowMap : rows) {
			int design_query_id = Integer.parseInt(rowMap.get("design_query_id"));
			String clause = rowMap.get("clause").toUpperCase();
			String expression_type = rowMap.get("expression_type").toUpperCase();
			String expression_string = rowMap.get("expression_string").toUpperCase();
			String query_text = rowMap.get("query_text").toUpperCase();
			
			Query_v1 query = null;
			if (queries.containsKey(design_query_id)) {
				query = queries.get(design_query_id);
			} else {
				query = new Query_v1();
				queries.put(design_query_id, query);
			}
			
			if (clause.equals("SELECT")) {
				if (expression_type.equals("TABLE_COLUMN")) {
					query.addSelect(expression_string);
				}					
			} else if (clause.equals("GROUP_BY")) {
				if (expression_type.equals("TABLE_COLUMN")) {
					query.addGroup_by(expression_string);
				}
			} else if (clause.equals("ORDER_BY")) {
				if (expression_type.equals("TABLE_COLUMN")) {
					query.addOrder_by(expression_string);
				}
			} else if (clause.equals("DURING_JOIN_FILTER") || clause.equals("POST_JOIN_FILTER") || clause.equals("PRE_JOIN_FILTER") || clause.equals("JOIN")) {
				query.addWhere(expression_string);
			}			
		}
		return queries;
	}


	@Override
	public void cluster(String input_query_log, String clustering_output)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
}
