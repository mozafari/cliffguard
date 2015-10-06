package edu.umich.robustopt.workloads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.Randomness;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaDeployer;
import edu.umich.robustopt.vertica.VerticaLatencyMeter;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;
import edu.umich.robustopt.workloads.EuclideanDistanceWithSimpleUnion.UnionOption;

public class EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile 
	extends LogFileBasedEuclideanDistanceWorkloadGeneratorUnionOfAllClauses {
	protected int numOfNewQueries;
	protected HashMap<Query, Long> latencys = null;
	private LatencyMeter latencyMeter = null;
	private double latencyPenaltyFactor;
	private boolean useExplainInsteadOfRunningQueries;


	public EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile(String dbName, String DBVendor, List<String> exampleSqlQueries,
			Set<UnionOption> whichClauses, int numOfNewQueries, LatencyMeter latencyMeter, Double latencyPenaltyFactor, boolean useExplainInsteadOfRunningQueries) throws Exception {
		super(dbName, DBVendor, exampleSqlQueries, whichClauses);
		this.numOfNewQueries = numOfNewQueries;
		this.latencyMeter = latencyMeter;
		this.latencyPenaltyFactor = latencyPenaltyFactor;
		this.useExplainInsteadOfRunningQueries = useExplainInsteadOfRunningQueries;
	}
	
	public EuclideanDistanceWithSimpleUnionAndLatency fast_distance(List<Query> leftWindow, 
			List<Query> rightWindow, HashMap<Query, Long> latencys, Double penaltyForGoingToNonZero, Double penaltyFactorForLatency) throws Exception {
		EuclideanDistanceWithSimpleUnion.Generator generator = 
				new EuclideanDistanceWithSimpleUnion.Generator(schema, penaltyForGoingToNonZero, whichClauses);
		EuclideanDistanceWithSimpleUnion distanceWithOutPenalty = generator.distance(leftWindow, rightWindow);
		long leftWindowSumLatency = 0L;
		for (Query q : leftWindow) {
			long oneLatency = latencys.get(q);
			leftWindowSumLatency = (oneLatency == Long.MAX_VALUE || leftWindowSumLatency == Long.MAX_VALUE  ? Long.MAX_VALUE : leftWindowSumLatency + oneLatency);
		}
		double leftDoubleSum = (leftWindowSumLatency == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : (double)leftWindowSumLatency);
		double leftAvgLatency = leftDoubleSum / leftWindow.size();
		
		long rightWindowSumLatency = 0L;
		for (Query q : rightWindow) {
			long oneLatency = latencys.get(q);
			rightWindowSumLatency = (oneLatency == Long.MAX_VALUE || rightWindowSumLatency == Long.MAX_VALUE  ? Long.MAX_VALUE : rightWindowSumLatency + oneLatency);
		}
		double rightDoubleSum = (rightWindowSumLatency == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : (double)rightWindowSumLatency);
		double rightAvgLatency = leftDoubleSum / rightWindow.size();
		Double distance;
		Double penaltyForLatency;
		if (leftAvgLatency == Long.MAX_VALUE && rightAvgLatency != Long.MAX_VALUE) {
			penaltyForLatency = 1d * penaltyFactorForLatency;
		} else if(leftAvgLatency != Long.MAX_VALUE && rightAvgLatency == Long.MAX_VALUE) {
			penaltyForLatency = 1d * penaltyFactorForLatency;
		} else if(leftAvgLatency == Long.MAX_VALUE && rightAvgLatency == Long.MAX_VALUE) {
			penaltyForLatency = 0d;
		} else {
			penaltyForLatency = normalize(leftAvgLatency, rightAvgLatency) * penaltyFactorForLatency;
		}
		distance = distanceWithOutPenalty.getDistance() * (1 - penaltyFactorForLatency) + penaltyForLatency;
		EuclideanDistanceWithSimpleUnionAndLatency dist = new EuclideanDistanceWithSimpleUnionAndLatency(distance, penaltyForGoingToNonZero, penaltyFactorForLatency, whichClauses);
		return dist;
	}
	
	
	static protected double normalize(double x, double y){
		if (x == 0d && y ==0d) {
			return 0d;
		}
		return Math.abs(x - y)/(x + y); 
	}

	@Override
	public ClusteredWindow forecastNextWindow(ClusteredWindow originalWindow, EuclideanDistance targetDistance) throws Exception {
		if (targetDistance == null) {
			throw new Exception("You need to provide a non-null EuclideanDistance");
		}
		EuclideanDistanceWithSimpleUnionAndLatency distance;
		if (targetDistance instanceof EuclideanDistanceWithSimpleUnionAndLatency) {
			distance = (EuclideanDistanceWithSimpleUnionAndLatency) targetDistance;
		} else {
			throw new Exception("The EuclideanDistance given must be of type EuclideanDistanceWithSimpleUnionAndLatency");
		}
		if (originalWindow == null) {
			throw new Exception("We cannot forecast the next window from a null window!");
		}
		penaltyForGoingFromZeroToNonZero = distance.getPenaltyForGoingFromZeroToNonZero();
		
		int listSize = exampleSqlQueries.size();
		
		double dist = targetDistance.getDistance();
		int originalWindowSize = originalWindow.totalNumberOfQueries();
		Map<String, Schema> schemaMap = getSchemaMap();
		EuclideanDistanceWithSimpleUnionAndLatency.Generator distanceGenerator = 
				new EuclideanDistanceWithSimpleUnionAndLatency.Generator(schemaMap , distance.getPenaltyForGoingFromZeroToNonZero(), 
						whichClauses, distance.getPenaltyForLatency(), latencyMeter, useExplainInsteadOfRunningQueries); 
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();

		List<Query> listOfOriginalWindow = originalWindow.getAllQueries();
		int numOfTrialsToFindBeta = 5000; // Change n here
		double muchLargerDistanceThanRequested = 0;
		List<Query> qList = new ArrayList<Query>();
		Set<Query> setOfCurWindow = new HashSet<Query>();
		List<Query> qStarList = new ArrayList<Query>();
		
		
		int count = 0;
		double bestSeenSofar = -1.0;
		
		// Populate set of current window
		for(Query q : listOfOriginalWindow){
			setOfCurWindow.add(q);
		}
		
		//store latency
		latencys = new HashMap<Query, Long>();
		List<PhysicalStructure> emptyDesign = new ArrayList<PhysicalStructure>();
		
		// Set of q in T\W1
		for (Query q : exampleSqlQueries){
			assert(!(q instanceof Query_SWGO));
			if (!latencys.containsKey(q)) {
				String query = q.getSql();
				List<String> oneQuery = new ArrayList<String>(1);
				oneQuery.add(query);
				long oneLatency = latencyMeter.measureSumLatency(oneQuery, emptyDesign, useExplainInsteadOfRunningQueries);
				if (oneLatency > 0) {
					latencys.put(q, oneLatency);
					qStarList.add(q);
				}
			}
		}
		
//		System.out.println("Done!");
//		System.out.println("Size of qStarList = " + qStarList.size());
//		System.out.println("Summary:" + qStarList.toString() + "\n\n");
		
		// set up new alpha
		Double alpha = dist, omega = distance.getPenaltyForLatency();
		List<Query> bestQList = new ArrayList<Query>();

		if (alpha >= omega) {
			dist = (alpha - omega) / (1 - omega);
		} else {
			dist = (alpha) / (1 - omega);
		}
		
		for (; count < numOfTrialsToFindBeta; count++) {
			qList = new ArrayList<Query>();
			for (int i = 0; i < numOfNewQueries; i++) {
				int randomC = Randomness.randGen.nextInt(listSize);
				Query q = qStarList.get(randomC);
				if(!setOfCurWindow.contains(q))
					qList.add(q);
				else
					i--;
			}
			EuclideanDistanceWithSimpleUnionAndLatency ed = fast_distance(qList, listOfOriginalWindow, latencys, distance.getPenaltyForGoingFromZeroToNonZero(), distance.getPenaltyForLatency());
			muchLargerDistanceThanRequested = ed.getDistance();
			if (alpha >= omega) {
				if (muchLargerDistanceThanRequested < dist) {
					bestSeenSofar = (bestSeenSofar < muchLargerDistanceThanRequested ? muchLargerDistanceThanRequested : bestSeenSofar);
				} else {
					if (bestSeenSofar < dist) {
						bestSeenSofar = muchLargerDistanceThanRequested;
					} else {
						bestSeenSofar = ((bestSeenSofar - dist) < (muchLargerDistanceThanRequested - dist) ? bestSeenSofar : muchLargerDistanceThanRequested);
					}
				}
			} else {
				bestSeenSofar = (bestSeenSofar < muchLargerDistanceThanRequested ? muchLargerDistanceThanRequested : bestSeenSofar);
			}
			if (bestSeenSofar == muchLargerDistanceThanRequested) {
				bestQList.addAll(qList);
			}
		}
		
		if ((bestSeenSofar < dist) || qList.isEmpty() || bestQList.isEmpty()) {
			throw new Exception("The log file is too similar with the current window or WorkloadGeneratorJingkui not applicable: " + bestSeenSofar + " < " + dist + "\nAnd CurrentWindow is " + originalWindow);
		}
		
		
//		System.out.println("Done!");
//		System.out.println("Size of bestQList = " + bestQList.size());
//		System.out.println("Summary:" + bestQList.toString() + "\n\n");

		//found a window with large distance
		double lambda = Math.sqrt(dist / bestSeenSofar);
		int copyN =(int) (originalWindowSize * lambda / ((1.0 - lambda) * numOfNewQueries));
		for (int k = 0; k < numOfNewQueries; k++) {
			Query q = bestQList.get(k);
			for(int i = 0; i < copyN; ++ i) {
				listOfOriginalWindow.add(q);
			}
		}
		
		if (listOfOriginalWindow.size() == originalWindowSize)
			throw new Exception("No new queries added!!");
		
		return clusteringQueryEquality.cluster(listOfOriginalWindow);
	}
	
	
	public static LatencyMeter createLatencyMeterForUnitTesting() throws Exception {
		String dbName = "dataset19";
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/";
		String configFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		List<DatabaseLoginConfiguration> allDatabaseConfigurations = DatabaseLoginConfiguration.loadDatabaseConfigurations(configFile, VerticaDatabaseLoginConfiguration.class.getSimpleName());
		String cacheFilename = topDir + "/experiment.cache";
		ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile(cacheFilename, 100, 1); // we do not want to re-write the whole cache after each latency meter!
		if (experimentCache==null)
			experimentCache = new ExperimentCache(cacheFilename, 100, 1, 1, new VerticaQueryPlanParser());//?
		
		DatabaseLoginConfiguration fullDB = DatabaseLoginConfiguration.getFullDB(allDatabaseConfigurations, dbName);
		
		DBDeployer dbDeployer = new VerticaDeployer(LogLevel.STATUS, fullDB , experimentCache, false);
							
		LatencyMeter latencyMeter = new VerticaLatencyMeter(LogLevel.STATUS, true, fullDB, 
						experimentCache, dbDeployer, dbDeployer, 10*60);
		
		return latencyMeter;
	}

	
	public static void unitTest1() throws Exception {
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String logFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "bigWindow";
		String s1 = "select * from st_etl_2.ident_164 where ident_164.ident_378 is null limit 10";
		String s2 = "select ident_2669, ident_2251, count(*) from st_etl_2.ident_164 group by 1,2 having count(*)>1";
		String s3 = "select ident_2251, count(*) from st_etl_2.ident_133 group by ident_2251 order by 1 desc";
		String s4 = "Select ident_1187, ident_2251 from st_etl_2.ident_133 where ident_2090>0 and ident_2090 is not null and ident_932 is null and ident_2251 in (274) group by ident_1187, ident_2251";
		List<String> w1 = new ArrayList<String>();
		w1.add(s1); w1.add(s1); w1.add(s2); w1.add(s3); w1.add(s3); w1.add(s4);
		List<Query_SWGO> wq1 = new Query_SWGO.QParser().convertSqlListToQuery(w1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(wq1);
		int maxQueriesPerWindow = 100;
		List<String> exampleQueries = SqlLogFileManager.loadQueryStringsFromPlainFile(logFile, maxQueriesPerWindow);
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		double penalty = 1.5d;
		EuclideanDistanceWithSimpleUnionAndLatency dist1 = new EuclideanDistanceWithSimpleUnionAndLatency(0.13, penalty, 0.1, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		//got everything set up
		int numOfNewQueries = 2;
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/";
		LatencyMeter latencyMeter = createLatencyMeterForUnitTesting();
		EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile workloadgenerator = new EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleQueries, option, numOfNewQueries, latencyMeter, 0.1, false);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(qlist1);
		System.out.println(window1);
		System.out.println("Start to forcast next query...");
		Timer t2 = new Timer();
		ClusteredWindow window2 = workloadgenerator.forecastNextWindow(window1,dist1);
		System.out.println("We spent " + t2.lapSeconds() + " seconds");
		List<Query> qlistWindow2 = window2.getAllQueries();
		EuclideanDistance distverify = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is "+distverify);
		System.out.println(window2);
		
		ClusteredWindow window3 = workloadgenerator.forecastNextWindow(window1,dist1);
		List<Query> qlistWindow3 = window3.getAllQueries();
		EuclideanDistanceWithSimpleUnion distverify2 = new EuclideanDistanceWithSimpleUnion.Generator(schemaMap, penalty, option).distance(qlist1, qlistWindow3);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify2);
		System.out.println(window3);
	}

	public static void unitTest35() throws Exception {
		Set<UnionOption> option = new HashSet<UnionOption> (){{  
	           add(UnionOption.SELECT);  
	           add(UnionOption.WHERE);  
	           add(UnionOption.GROUP_BY);
	           add(UnionOption.ORDER_BY);
		}};
		
		String dbName = "dataset19";
		Map<String, Schema> schemaMap = SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VerticaDatabaseLoginConfiguration.class.getSimpleName()).getSchemas();
		String topDir = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/";
		String dbConfigFile = GlobalConfigurations.RO_BASE_PATH + "/databases.conf"; 
		String unionSqlQueriesFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/" + "parsed-runnable-improvable.timestamped";
		String windowFile = GlobalConfigurations.RO_BASE_PATH + "/processed_workloads/real/dataset19/dvals/d0-4.945309816428576E-4/" + "w0.queries";
		LatencyMeter latencyMeter = null;
		DBDesigner dbDesigner = null;
		SqlLogFileManager<Query_SWGO> sqlLogFileManager = new SqlLogFileManager<Query_SWGO>('|', "\n", new Query_SWGO.QParser(), schemaMap, latencyMeter, true, dbDesigner);
		List<Query_SWGO> exampleQueries = sqlLogFileManager.loadTimestampQueriesFromFile(unionSqlQueriesFile);
		List<String> exampleSqlQueries = new ArrayList<String>();
		for (Query_SWGO q : exampleQueries)
			exampleSqlQueries.add(q.getSql());
		
		List<String> wq1 = sqlLogFileManager.loadQueryStringsFromPlainFile(windowFile, 10000);
		List<Query_SWGO> swgo_qlist1 = new Query_SWGO.QParser().convertSqlListToQuery(wq1, schemaMap);
		List<Query> qlist1 = Query.convertToListOfQuery(swgo_qlist1);
		double avgDistance = 0.01;
		double penaltyForGoingFromZeroToNonZero = 1.0;
		double latencyPenalty = 0.2;
		
		EuclideanDistanceWithSimpleUnionAndLatency dist1 = new EuclideanDistanceWithSimpleUnionAndLatency(avgDistance, penaltyForGoingFromZeroToNonZero, latencyPenalty, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionShiyong.AllClausesOption);
		int numOfNewQueries = 3;
		LatencyMeter realLatencyMeter = createLatencyMeterForUnitTesting();
		//EuclideanDistanceWorkloadGenerator workloadgenerator = new EuclideanDistanceWorkloadGenerator(schemaMap,null, 3);
		EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile workloadgenerator = new EuclideanDistanceWithLatencyWorkloadGeneratorFromLogFile("dataset19", VerticaDatabaseLoginConfiguration.class.getSimpleName(), exampleSqlQueries, option, numOfNewQueries, realLatencyMeter, 0.1d, false);
		Clustering_QueryEquality clusteringQueryEquality = new Clustering_QueryEquality();
		ClusteredWindow window1 = clusteringQueryEquality.cluster(swgo_qlist1);
		System.out.println(window1);
		System.out.println("Start to forecast next query..."); 
		Timer t = new Timer();
		ClusteredWindow window2 = null;
		window2 = workloadgenerator.forecastNextWindow(window1, dist1);
			
		System.out.println("We spent " + t.lapSeconds() + " seconds");
		List<Query> qlistWindow2 = window2.getAllQueries();
		boolean useExplainInsteadOfRunningQueries = true;
		EuclideanDistanceWithSimpleUnionAndLatency distverify = new 
				EuclideanDistanceWithSimpleUnionAndLatency.Generator(schemaMap, penaltyForGoingFromZeroToNonZero, option, latencyPenalty, realLatencyMeter, useExplainInsteadOfRunningQueries).distance(qlist1, qlistWindow2);
		System.out.println("distance1 = " + dist1);
		System.out.println("We generated a window with distance1 from win1, and its actual distance is " + distverify);
		System.out.println(window2);
	}
	
	public static void main(String args[]) {
		try {
			unitTest35();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public DistributionDistanceGenerator<EuclideanDistance> getDistributionDistanceGenerator()
			throws Exception {
		return new EuclideanDistanceWithSimpleUnionAndLatency.Generator(schema, penaltyForGoingFromZeroToNonZero, whichClauses, 
				latencyPenaltyFactor, latencyMeter, useExplainInsteadOfRunningQueries);
	}

	public void setLatencyMeter(LatencyMeter latencyMeter) {
		this.latencyMeter = latencyMeter;
	}
}
