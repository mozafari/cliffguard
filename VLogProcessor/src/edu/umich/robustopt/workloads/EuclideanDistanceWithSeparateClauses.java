package edu.umich.robustopt.workloads;
 
import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.clustering.Query_SWGO.QParser;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.MyMathUtils;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Vector;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;

public class EuclideanDistanceWithSeparateClauses extends EuclideanDistance {
	private static final long serialVersionUID = -495986816860028860L;
	
	public EuclideanDistanceWithSeparateClauses	(Double dist, Double penaltyForGoingFromZeroToNonZero, Integer numOfPairsRepresentedByThisObject) throws Exception {
		super(dist, penaltyForGoingFromZeroToNonZero, numOfPairsRepresentedByThisObject);
	}
	
	public EuclideanDistanceWithSeparateClauses(Double dist, Double penaltyForGoingFromZeroToNonZero) throws Exception {
		this(dist, penaltyForGoingFromZeroToNonZero, 1);
	}
	
	public EuclideanDistanceWithSeparateClauses(Double dist) throws Exception {
		this(dist, 1d, 1);
	}
	
	public EuclideanDistanceWithSeparateClauses(EuclideanDistanceWithSeparateClauses first) throws Exception {
		this(first.euclideanDistance, first.penaltyForGoingFromZeroToNonZero,
			 first.howManyPairsAreRepresentedByThisObject);
	}
	
	@Override
	public EuclideanDistance makeCopy(Double newDistanceValue) throws Exception {
		return new EuclideanDistanceWithSeparateClauses(newDistanceValue, this.penaltyForGoingFromZeroToNonZero, this.howManyPairsAreRepresentedByThisObject);
	}

	@Override
	public String showSummary() {
		return "EuclideanDistanceWithSeparateClauses <" + euclideanDistance + ">, penaltyForGoingFromZeroToNonZero <" + penaltyForGoingFromZeroToNonZero + ">";
	}
	
	@Override
	public Double getDistance() {
		return euclideanDistance;
	}

	
	@Override
	public DistributionDistance computeAverage(DistributionDistance firstD,
			DistributionDistance secondD) throws Exception {
		if (!(firstD instanceof EuclideanDistanceWithSeparateClauses) 
				|| !(secondD instanceof EuclideanDistanceWithSeparateClauses)) {
			throw new Exception("Cannot average incompatible types of distances: " 
								+ firstD.getClass().getCanonicalName() + " and "
								+ secondD.getClass().getCanonicalName());
			}
		EuclideanDistanceWithSeparateClauses first = (EuclideanDistanceWithSeparateClauses) firstD;
		EuclideanDistanceWithSeparateClauses second = (EuclideanDistanceWithSeparateClauses) secondD;
		if (Math.abs(first.penaltyForGoingFromZeroToNonZero - second.penaltyForGoingFromZeroToNonZero)>0.00000001) {
			throw new Exception("Can't computeAverage when the first distance's penalty factor is "
					+ first.getPenaltyForGoingFromZeroToNonZero() + " and the second distance's penalty factor is "
					+ second.getPenaltyForGoingFromZeroToNonZero());
		}
		int n1 = first.howManyPairsAreRepresentedByThisObject;
		int n2 = second.howManyPairsAreRepresentedByThisObject;
		Double avgDistance = MyMathUtils.weightedAvg(n1, first.euclideanDistance, n2, second.euclideanDistance);
		EuclideanDistanceWithSeparateClauses result = new EuclideanDistanceWithSeparateClauses(first); //penalty gets copied automatically
		result.euclideanDistance = avgDistance;
		result.howManyPairsAreRepresentedByThisObject = n1 + n2;
		return result;
	}
	
	public static class Generator extends EuclideanDistance.Generator {		
		
		public Generator(Map<String, Schema> schemaMap, Double penaltyFactor) throws Exception {
			super(schemaMap, penaltyFactor);
		}
		
		public Generator(Map<String, Schema> schemaMap) throws Exception {
			this(schemaMap, 1d);
		}
		
		@Override
		public EuclideanDistanceWithSeparateClauses distance(List<Query> leftWindow,
				List<Query> rightWindow) throws Exception {
			List<Query_SWGO> leftQueryList = new ArrayList<Query_SWGO>();
			List<Query_SWGO> rightQueryList = new ArrayList<Query_SWGO>();
			for (Query q : leftWindow) {
				if (q instanceof Query_SWGO) {
					leftQueryList.add((Query_SWGO)q);
				} else {
					throw new Exception("only support Query_SWGO. Given query type in the left window was " + q.getClass().getCanonicalName());
				}
			}

			for (Query q : rightWindow) {
				if (q instanceof Query_SWGO) {
					rightQueryList.add((Query_SWGO)q);
				} else {
					throw new Exception("only support Query_SWGO. Given query type in the right window was " + q.getClass().getCanonicalName());
				}
			}

			Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
			ClusteredWindow leftClusters = clusteringQueryEquality.cluster(leftQueryList);
			ClusteredWindow rightClusters = clusteringQueryEquality.cluster(rightQueryList);
			EuclideanDistanceWithSeparateClauses dist = distanceForClusteredWindows(leftClusters, rightClusters);
			return dist;
		} 

				
		protected EuclideanDistanceWithSeparateClauses distanceForClusteredWindows(ClusteredWindow leftWindow, ClusteredWindow rightWindow) throws Exception {
			if (leftWindow == null || rightWindow == null) {
				throw new Exception("Input window should not be null: " + (leftWindow == null) + " and " + (rightWindow == null));
			}
			Double distance;
			HashMap<Vector<Boolean>, Double> leftWindowHashMap = convertClusteredWindowToHashMap(leftWindow);
			HashMap<Vector<Boolean>, Double> rightWindowHashMap = convertClusteredWindowToHashMap(rightWindow);
			distance = getEuclideanDistance(leftWindowHashMap, rightWindowHashMap);
			EuclideanDistanceWithSeparateClauses dist = new EuclideanDistanceWithSeparateClauses(distance, generatorPenaltyForGoingFromZeroToNonZero);
			return dist;
		}
		
		@Override
		protected Vector<Boolean> convertClusterToBinaryVector(Cluster c)
				throws Exception {
			int numberOfColumns = getNumOfColumns();
			Vector<Boolean> result = new Vector<Boolean>(4*numberOfColumns); // we need to use a 4*n bit vector to represent columns in all 4 clauses!
			//initialize result
			Query q = c.retrieveAQueryAtPosition(0);
			Query_SWGO query;
			if (! (q instanceof Query_SWGO)) {
				throw new Exception("Current EuclideanDistance only supports Query_SWGO. Input query in convertClusterToBinaryVector was of type " + q.getClass().getCanonicalName() + " and the query itself is: " + q);
			} else {
				query = (Query_SWGO)q;
			}

			Set<ColumnDescriptor> selectCols = query.getSelect();
			if (!convertClauseToBinary(selectCols, result))
				System.err.println("Converting columns from the " + "select" + " clause failed for cluster " + c);
			Set<ColumnDescriptor> whereCols = query.getWhere();
			if (!convertClauseToBinary(whereCols, result))
				System.err.println("Converting columns from the " + "where" + " clause failed for cluster " + c);
			Set<ColumnDescriptor> groupByCols = new HashSet(query.getGroup_by());
			if (!convertClauseToBinary(groupByCols, result))
				System.err.println("Converting columns from the " + "group by" + " clause failed for cluster " + c);
			Set<ColumnDescriptor> orderByCols = new HashSet(query.getOrder_by());
			if (!convertClauseToBinary(orderByCols, result))
				System.err.println("Converting columns from the " + "order by" + " clause failed for cluster " + c);
			
			return result;
		}
		
		private boolean convertClauseToBinary(Set<ColumnDescriptor> columnsInOneClause, Vector<Boolean> result) {
			int numberOfColumns = getNumOfColumns();
			int numberOfClauseColumns = columnsInOneClause.size();
			int numberOfMatchingColumns = 0;
			for (int i = 0; i < numberOfColumns; i++){
				ColumnDescriptor column = dbColumns.get(i);
				if (columnsInOneClause.contains(column)){
					result.add(true);
					++ numberOfMatchingColumns;
				} else {
					result.add(false);
				}
			}
			if (numberOfMatchingColumns < numberOfClauseColumns) { 
				System.err.println("convertClusterToBinaryVector: error: out of the " + numberOfClauseColumns + " columns we only recognized " + numberOfMatchingColumns + " columns. " 
						+ "The set of columns was " + columnsInOneClause + "\nand the cluster was ");
				return false;
			}
			if (numberOfMatchingColumns == 0 && numberOfClauseColumns>0) {
				System.err.println("The given cluster has zero matching columns the current clause: " + columnsInOneClause);
				return false;
			}

			return true;		
		}

	}
		
	
	public static void unitTest1() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("tpch", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		
		String s1 = "SELECT l_partkey FROM lineitem WHERE l_shipmode>1 GROUP BY l_comment ORDER BY l_shipinstruct;";
		String s2 = "SELECT l_shipmode FROM lineitem WHERE l_comment>1 AND l_partkey>1 GROUP BY l_shipinstruct;";
		String s3 = "SELECT l_comment FROM lineitem WHERE l_returnflag>1;";
		String s4 = "SELECT l_returnflag FROM lineitem WHERE l_comment>3;";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		
		List<String> w2 = new ArrayList<String>();
		w2.add(s1); w2.add(s3); w2.add(s3); w2.add(s4);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		List<Query> qlist2 = Query.convertToListOfQuery(wq2);

		EuclideanDistanceWithSeparateClauses dist1 = (EuclideanDistanceWithSeparateClauses) new EuclideanDistanceWithSeparateClauses.Generator(schemaMap).distance(qlist1, qlist2);
		System.out.println(dist1.showSummary());
		
		List<String> w3 = new ArrayList<String>();
		w3.add(s1); w3.add(s3); w3.add(s3); 
		List<Query_SWGO> wq3 = new Query_SWGO.QParser().convertSqlListToQuery(w3, schemaMap);
		List<Query> qlist3 = Query.convertToListOfQuery(wq3);
		EuclideanDistanceWithSeparateClauses dist2 = (EuclideanDistanceWithSeparateClauses) new EuclideanDistanceWithSeparateClauses.Generator(schemaMap).distance(qlist1, qlist3);
		System.out.println(dist2.showSummary());
		
		DistributionDistance dist3 = dist1.computeAverage(dist1, dist2);
		System.out.println(dist3.showSummary());
	}
	

	public static void unitTest2() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String windowFile1 = GlobalConfigurations.RO_BASE_PATH + "/Shiyong_test/" + "dataset19_queries_1.txt";
		int maxQueriesPerWindow = 100;
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<String> w1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile1, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		String windowFile2 = GlobalConfigurations.RO_BASE_PATH + "/Shiyong_test/" + "dataset19_queries_2.txt";
		List<String> w2 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile2, maxQueriesPerWindow);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		List<Query> qlist2 = Query.convertToListOfQuery(wq2);
		Timer t = new Timer();
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap).distance(qlist1,qlist2);
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		System.out.println(dist1);
		//System.out.println(dist2);
	}

	public static void unitTest3() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("wide", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String windowFile1 = GlobalConfigurations.RO_BASE_PATH + "/Shiyong_test/" + "wide_window_1.txt";
		int maxQueriesPerWindow = 150;
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap);
		List<String> w1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile1, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		String windowFile2 = GlobalConfigurations.RO_BASE_PATH + "/Shiyong_test/" + "wide_window_2.txt";
		List<String> w2 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile2, maxQueriesPerWindow);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		List<Query> qlist2 = Query.convertToListOfQuery(wq2);
		double penalty = 1.5d;
		Timer t = new Timer();
		EuclideanDistanceWithSeparateClauses dist1 = new EuclideanDistanceWithSeparateClauses.Generator(schemaMap, penalty).distance(qlist1,qlist2);
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		System.out.println(dist1);
		//System.out.println(dist2);
	}
	
	public static void main(String args[]) {
		try {
			unitTest1();
//			unitTest2();
			unitTest3();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
