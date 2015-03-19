package edu.umich.robustopt.workloads;

import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.clustering.Query_SWGO.QParser;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.MyMathUtils;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.*;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Vector;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.sun.xml.internal.bind.util.Which;

public class EuclideanDistanceWithSimpleUnionAndLatency extends EuclideanDistanceWithSimpleUnion {
	private static final long serialVersionUID = 7979916503356588276L;
	protected double latencyPenaltyFactor;

	public EuclideanDistanceWithSimpleUnionAndLatency (Double dist, Double penaltyFactor, Integer numOfPairsRepresentedByThisObject, Double latencyPenaltyFactor, Set<UnionOption> whichClauses) throws Exception {
		super(dist, penaltyFactor, numOfPairsRepresentedByThisObject, whichClauses);
		if (latencyPenaltyFactor < 0d) {
			throw new Exception("latencyPenaltyFactor should not be negative.");
		}
		this.latencyPenaltyFactor = latencyPenaltyFactor;
	}
	
	public EuclideanDistanceWithSimpleUnionAndLatency (Double dist, Double penaltyFactor, Double latencyPenaltyFactor, Set<UnionOption> whichClauses) throws Exception {
		this(dist, penaltyFactor, 1, latencyPenaltyFactor, whichClauses);
	}

	public EuclideanDistanceWithSimpleUnionAndLatency (Double dist) throws Exception {
		this(dist, 1d, 1, 0d, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
	}

	public EuclideanDistanceWithSimpleUnionAndLatency (EuclideanDistanceWithSimpleUnionAndLatency first) throws Exception {
		this(first.euclideanDistance, first.penaltyForGoingFromZeroToNonZero,
				first.howManyPairsAreRepresentedByThisObject, first.latencyPenaltyFactor, first.whichClauses);
	}

	public double getPenaltyForLatency(){	
		return latencyPenaltyFactor;
	}

	@Override
	public Double getDistance() {
		return euclideanDistance; // euclideanDistance at this point contains both distance and latency terms in it!
	}

	@Override
	public DistributionDistance computeAverage(DistributionDistance first, DistributionDistance second) throws Exception {
		if (!(first instanceof EuclideanDistanceWithSimpleUnionAndLatency) 
				|| !(second instanceof EuclideanDistanceWithSimpleUnionAndLatency)) {
			throw new Exception("Cannot average incompatible types of distances: " 
					+ first.getClass().getCanonicalName() + " and "
					+ second.getClass().getCanonicalName());
		}
		EuclideanDistanceWithSimpleUnionAndLatency firstD = (EuclideanDistanceWithSimpleUnionAndLatency) first;
		EuclideanDistanceWithSimpleUnionAndLatency secondD = (EuclideanDistanceWithSimpleUnionAndLatency) second;
		double penaltyFactor1 = firstD.getPenaltyForGoingFromZeroToNonZero();
		double penaltyFactor2 = secondD.getPenaltyForGoingFromZeroToNonZero();
		if (Math.abs(penaltyFactor1 - penaltyFactor2) > 0.000001) {
			throw new Exception("Can't computeAverage when the first distance's penalty factor is "
					+ firstD.getPenaltyForGoingFromZeroToNonZero() + " and the second distance's penalty factor is "
					+ secondD.getPenaltyForGoingFromZeroToNonZero());
		}
		
		double penaltyForLatency1 = firstD.getPenaltyForLatency(); 
		double penaltyForLatency2 = secondD.getPenaltyForLatency();
		if (Math.abs(penaltyForLatency1 - penaltyForLatency2) > 0.000001){	
			throw new Exception("Can't computeAverage when the first latency's penalty factor is "
					+ firstD.getPenaltyForGoingFromZeroToNonZero() + " and the second distance's penalty factor is "
					+ secondD.getPenaltyForGoingFromZeroToNonZero());
		}
		int n1 = firstD.howManyPairsAreRepresentedByThisObject;
		int n2 = secondD.howManyPairsAreRepresentedByThisObject;
		Double avgDistance = MyMathUtils.weightedAvg(n1, firstD.euclideanDistance, n2, secondD.euclideanDistance);
		EuclideanDistanceWithSimpleUnionAndLatency result = new EuclideanDistanceWithSimpleUnionAndLatency(firstD);//
		result.euclideanDistance = avgDistance;
		result.howManyPairsAreRepresentedByThisObject = n1 + n2;
		return result;
	}
	
	@Override
	public String showSummary() {
		return "EuclideanDistanceWithSimpleUnionAndLatency <overallDistance=" + getDistance() +  ", penaltyForGoingFromZeroToNonZero=" + penaltyForGoingFromZeroToNonZero + 
		", whichClauses=" + whichClauses + ", latencyPenaltyFactor=" + latencyPenaltyFactor;
	}
	
	public static class Generator extends EuclideanDistanceWithSimpleUnion.Generator {
		protected double latencyPenaltyFactor;
		private LatencyMeter latencyMeter = null;
		private boolean useExplainInsteadOfRunningQueries = true;
		public Generator(Map<String, Schema> schemaMap, Double penaltyForGoingFromZeroToNonZero, Set<UnionOption> option, 
				Double latencyPenaltyFactor, LatencyMeter latencyMeter, boolean useExplainInsteadOfRunningQueries) throws Exception {
			super(schemaMap, penaltyForGoingFromZeroToNonZero, option);
			if (latencyPenaltyFactor < 0d) {
				throw new Exception("latencyPenaltyFactor should not be negative.");
			}
			this.latencyPenaltyFactor = latencyPenaltyFactor;
			this.latencyMeter = latencyMeter;
			this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
		}
		
		public Generator(Map<String, Schema> schemaMap, Set<UnionOption> option, 
				Double latencyPenaltyFactor, LatencyMeter latencyMeter, boolean useExplainInsteadOfRunningQueries) throws Exception {
			this(schemaMap, 1d, option, latencyPenaltyFactor, latencyMeter, useExplainInsteadOfRunningQueries);
		}
		
		static protected double normalize(double x, double y){
			if (x == 0d && y ==0d) {
				return 0d;
			}
			return Math.abs(x - y)/(x + y); 
		}
		
		public Double getLatencyPenaltyFactor() {
			return latencyPenaltyFactor;
		}
		
		@Override
		public EuclideanDistanceWithSimpleUnionAndLatency distance(List<Query> leftWindow, List<Query> rightWindow) throws Exception {
			if (leftWindow == null || rightWindow == null) {
				throw new Exception("Input window should not be null");
			}
			Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
			ClusteredWindow unchangedLeftWindow = clusteringQueryEquality.cluster(leftWindow);
			ClusteredWindow unchangedRightWindow = clusteringQueryEquality.cluster(rightWindow);
			
			List<Query_SWGO> leftQueryList = new ArrayList<Query_SWGO>();
			List<Query_SWGO> rightQueryList = new ArrayList<Query_SWGO>();
			for (Query q : leftWindow) {
				if (q instanceof Query_SWGO) {
					leftQueryList.add((Query_SWGO)q);
				} else {
					throw new Exception("only support Query_SWGO");
				}
			}

			for (Query q : rightWindow) {
				if (q instanceof Query_SWGO) {
					rightQueryList.add((Query_SWGO)q);
				} else {
					throw new Exception("only support Query_SWGO");
				}
			}

			ClusteredWindow leftClusters = clusteringQueryEquality.cluster(leftQueryList);
			ClusteredWindow rightClusters = clusteringQueryEquality.cluster(rightQueryList);
			EuclideanDistanceWithSimpleUnion distanceWithoutLatency = super.distanceForClusteredWindows(leftClusters, rightClusters);
			
			Double distance = distanceWithoutLatency.getDistance();
			if (Math.abs(latencyPenaltyFactor) > 0.000001) {
				List<PhysicalStructure> design = new ArrayList<PhysicalStructure>();
				boolean useOnlyFirstQuery = false;

				Long latencyOfLeftWindow = latencyMeter.measureAvgLatencyForOneClusteredWindow(unchangedLeftWindow, design, useOnlyFirstQuery, useExplainInsteadOfRunningQueries);
				Long latencyOfRightWindow = latencyMeter.measureAvgLatencyForOneClusteredWindow(unchangedRightWindow, design, useOnlyFirstQuery, useExplainInsteadOfRunningQueries);
				Double penaltyForLatency;
				if (latencyOfLeftWindow == Long.MAX_VALUE && latencyOfRightWindow != Long.MAX_VALUE) {
					penaltyForLatency = 1d * latencyPenaltyFactor;
				} else if(latencyOfLeftWindow != Long.MAX_VALUE && latencyOfRightWindow == Long.MAX_VALUE) {
					penaltyForLatency = 1d * latencyPenaltyFactor;
				} else if(latencyOfLeftWindow == Long.MAX_VALUE && latencyOfRightWindow == Long.MAX_VALUE) {
					penaltyForLatency = 0d;
				} else {
					penaltyForLatency = normalize(latencyOfLeftWindow, latencyOfRightWindow) * latencyPenaltyFactor;
				}
				distance = distance * (1 - latencyPenaltyFactor) + penaltyForLatency;
			}
			// the distance at this point has both terms in it!
			EuclideanDistanceWithSimpleUnionAndLatency dist = new EuclideanDistanceWithSimpleUnionAndLatency(distance, generatorPenaltyForGoingFromZeroToNonZero, latencyPenaltyFactor, whichClauses);
			
			return dist;
		}		
	}
	
	public static LatencyMeter createLatencyMeterForUnitTesting() throws Exception {
		String dbAlias = "dataset19";
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/";
		String configFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		List<DatabaseLoginConfiguration> allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(configFile, VerticaDatabaseLoginConfiguration.class.getSimpleName());
		String cacheFilename = topDir + "/experiment.cache";
		ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile(cacheFilename, 100, 1); // we do not want to re-write the whole cache after each latency meter!
		if (experimentCache==null)
			experimentCache = new ExperimentCache(cacheFilename, 100, 1, 1, new VerticaQueryPlanParser());//?
		DatabaseLoginConfiguration fullDB = DatabaseLoginConfiguration.getFullDB(allDatabaseConfigurations, dbAlias);
		DBDeployer dbDeployer = new VerticaDeployer(LogLevel.STATUS, fullDB , experimentCache, false);
							
		LatencyMeter latencyMeter = new VerticaLatencyMeter(LogLevel.STATUS, true, fullDB, 
						experimentCache, dbDeployer, dbDeployer, 10*60);
		
		return latencyMeter;
	}
	
	public static void unitTest1() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		
		String s1 = "select * from st_etl_2.ident_164 where ident_164.ident_378 is null limit 10";
		String s2 = "select ident_2669, ident_2251, count(*) from st_etl_2.ident_164 group by 1,2 having count(*)>1";
		String s3 = "select ident_2251, count(*) from st_etl_2.ident_133 group by ident_2251 order by 1 desc";
		String s4 = "Select ident_1187, ident_2251 from st_etl_2.ident_133 where ident_2090>0 and ident_2090 is not null and ident_932 is null and ident_2251 in (274) group by ident_1187, ident_2251";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		
		List<String> w2 = new ArrayList<String>();
		w2.add(s1); w2.add(s3); w2.add(s3); w2.add(s4);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		List<Query> qlist2 = Query.convertToListOfQuery(wq2);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		EuclideanDistanceWithSimpleUnion dist1 = (EuclideanDistanceWithSimpleUnion) 
				new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, option).distance(qlist1, qlist2);
		System.out.println(dist1.showSummary());
		
		List<String> w3 = new ArrayList<String>();
		w3.add(s1); w3.add(s3); w3.add(s3); 
		List<Query_SWGO> wq3 = new Query_SWGO.QParser().convertSqlListToQuery(w3, schemaMap);
		List<Query> qlist3 = Query.convertToListOfQuery(wq3);
		
		LatencyMeter latencyMeter = createLatencyMeterForUnitTesting();
		
		EuclideanDistanceWithSimpleUnionAndLatency dist2 = (EuclideanDistanceWithSimpleUnionAndLatency) 
				new EuclideanDistanceWithSimpleUnionAndLatency.Generator(schemaMap, 1.0, option, 0.1, latencyMeter, true).distance(qlist1, qlist3);
		System.out.println(dist2.showSummary());
		
		DistributionDistance dist3 = dist1.computeAverage(dist1, dist2);
		System.out.println(dist3.showSummary());
	}
	

	public static void unitTest2() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String windowFile1 = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w1.queries";
		int maxQueriesPerWindow = 100;
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', ";barzan", new Query_SWGO.QParser(), schemaMap);
		List<String> w1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile1, maxQueriesPerWindow);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		String windowFile2 = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w2.queries";
		List<String> w2 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile2, maxQueriesPerWindow);
		List<Query_SWGO> wq2 = new Query_SWGO.QParser().convertSqlListToQuery(w2, schemaMap);
		List<Query> qlist2 = Query.convertToListOfQuery(wq2);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		Timer t = new Timer();

		LatencyMeter latencyMeter = createLatencyMeterForUnitTesting();
		
		EuclideanDistanceWithSimpleUnionAndLatency dist1 = (EuclideanDistanceWithSimpleUnionAndLatency) 
				new EuclideanDistanceWithSimpleUnionAndLatency.Generator(schemaMap, 1.0, option, 0.1, latencyMeter, true).distance(qlist1, qlist2);
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		System.out.println(dist1);
		//System.out.println(dist2);
	}
	public static void main(String args[]) {
		try {
			unitTest1();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
