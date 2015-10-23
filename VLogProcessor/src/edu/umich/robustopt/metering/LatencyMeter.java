package edu.umich.robustopt.metering;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.PerformanceValue;
import edu.umich.robustopt.metering.ExperimentCache.PerformanceKey;
import edu.umich.robustopt.physicalstructures.*;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaConnection;
import edu.umich.robustopt.vertica.VerticaDeployer;
import edu.umich.robustopt.vertica.VerticaLatencyMeter;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;

public abstract class LatencyMeter {
	protected Connection queryConnection;
	protected Connection controlConnection;
	protected DatabaseLoginConfiguration databaseLogin;
	protected boolean emptyCacheForEachRun;
	protected ExperimentCache experimentCache;
	protected DatabaseInstance dbDeployment;
	protected DBDeployer dbDeployer;
	protected BLog log;
//	private boolean useExplainInsteadOfRunningQueries;
	protected long howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite = 10 * 60; // 10 minutes
	
	protected transient long secondsSpentWaitingForMissingStructuresToBeDeployed = 0;
	protected transient long secondsSpentMeasuringQueryLatency = 0;
	protected transient long secondsSpentProducingQueryPlans = 0;
	protected transient long secondsSpentEmptyingCache = 0;
	protected transient long numberOfCacheEmptying = 0;
	protected transient long numberOfCasesWithActualMeasuring = 0;
	protected transient long numberOfCasesWithPreviouslySeenPlan = 0;
	protected transient long numberOfCasesIdenticalWithPreviousCases= 0;
	
	public LatencyMeter (LogLevel verbosity, boolean emptyCacheForEachRun, DatabaseLoginConfiguration databaseLogin, ExperimentCache experimentCache, DatabaseInstance dbDeployment, DBDeployer dbDeployer, long howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite) throws Exception {
		this.emptyCacheForEachRun = emptyCacheForEachRun;
		this.databaseLogin = databaseLogin;
		this.queryConnection = databaseLogin.createConnection();
		this.controlConnection = databaseLogin.createConnection();
		this.experimentCache = experimentCache;
		this.dbDeployment = dbDeployment;
		this.dbDeployer = dbDeployer;
		this.log = new BLog(verbosity);
		this.howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite = howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite;
	}
	
	// should empty both the DB cache as well as the OS cache!
	public abstract void emptyCache() throws Exception;

	/*
	 * return time is in miliseconds or optimizer cost units
	 */
	public long measureSumLatency(List<String> queries, List<PhysicalStructure> whichProjectionsToInclude, boolean useExplainInsteadOfRunningQueries) throws Exception {
		return measureSumLatency(queries, whichProjectionsToInclude, 1, useExplainInsteadOfRunningQueries);
	}

	/*
	 * return time is in miliseconds or optimizer cost units
	 */
	public long measureSumLatency(List<String> queries, List<PhysicalStructure> whichProjectionsToInclude, int numberOfRepetitions, boolean useExplainInsteadOfRunningQueries) throws Exception {
		long sumLatency = 0;
		for (String query : queries) {
			PerformanceRecord performanceRecord = new PerformanceRecord(query);
			measureLatency(performanceRecord, whichProjectionsToInclude, "test", numberOfRepetitions, useExplainInsteadOfRunningQueries);
			long thisLatency;
			if (useExplainInsteadOfRunningQueries)
				thisLatency = performanceRecord.getPerformanceValueWithDesign("test").getMeanOptimizerCost();
			else
				thisLatency = performanceRecord.getPerformanceValueWithDesign("test").getMeanActualLatency();
			sumLatency = (thisLatency == Long.MAX_VALUE || sumLatency == Long.MAX_VALUE  ? Long.MAX_VALUE : sumLatency+thisLatency); 
		}
		return sumLatency;
	}
	
	/**
	 * return time is in miliseconds or optimizer cost units
	 * @param query
	 * @param physicalProjectionsToInclude
	 * @return
	 * @throws SQLException
	 */
	public void measureLatency(PerformanceRecord performanceRecord, List<PhysicalStructure> physicalStructuresToInclude, String designAlgorithmName, boolean useExplainInsteadOfRunningQueries) throws Exception {
		measureLatency(performanceRecord, physicalStructuresToInclude, designAlgorithmName, 1, useExplainInsteadOfRunningQueries);
	}

	
	/**
	 * return time is in miliseconds or optimizer cost units
	 * @param query
	 * @param physicalProjectionsToInclude
	 * @return
	 * @throws SQLException
	 */			
		
	public void measureLatency(PerformanceRecord performanceRecord, List<PhysicalStructure> physicalStructuresToInclude, 
			String designAlgorithmName, int numberOfRepetitions, boolean useExplainInsteadOfRunningQueries) throws Exception {
		assert (numberOfRepetitions>=1);

		String query = performanceRecord.getQuery();		
		PerformanceValue performanceValue;
		boolean needsAccurate = !useExplainInsteadOfRunningQueries;
		
		if (experimentCache!=null && experimentCache.getPerformance(query, new PhysicalDesign(physicalStructuresToInclude), needsAccurate) != null) {
			performanceValue = experimentCache.getPerformance(query, new PhysicalDesign(physicalStructuresToInclude), needsAccurate);
			log.status(LogLevel.DEBUG, "performance value with this allowed projections fetched from cache.");
			++numberOfCasesIdenticalWithPreviousCases;
		} else { // we need to measure those!
			Timer t1 = new Timer();
			String theFirstQueryPlan = getQueryPlan(query, physicalStructuresToInclude);
			secondsSpentProducingQueryPlans += t1.lapSeconds();
			
			if (experimentCache!=null && experimentCache.getPerformance(query, theFirstQueryPlan, needsAccurate) !=null) {
				performanceValue = experimentCache.getPerformance(query, theFirstQueryPlan, needsAccurate);
				log.status(LogLevel.DEBUG, "performance value with this query plan fetched from cache.");
				++ numberOfCasesWithPreviouslySeenPlan;
			} else {
				Timer t2 = new Timer();
				log.status(LogLevel.DEBUG, "performance value with this query plan was NOT found in cache.");
				List<Long> allDurations = new ArrayList<Long>();
				List<Long> allOptimizerCosts = new ArrayList<Long>();
				List<String> allQueryPlans = new ArrayList<String>();
				
				for (int rep = 0; rep < numberOfRepetitions; ++ rep) {
					if (emptyCacheForEachRun)
						emptyCache();
					
					PlanEstimate pe = getQueryPlanAndCostEstimate(query, physicalStructuresToInclude);

					allQueryPlans.add(pe.getQueryPlan());
					allOptimizerCosts.add(pe.getOptmizerCostEstimate());
					
					boolean failed = false;
					String failureMessage = null;
					if (!useExplainInsteadOfRunningQueries) {
						
						log.status(LogLevel.DEBUG, "Executing: " + query);
						try {
							//run your query here!
							long duration = measureQueryLatencyInMilliSecondsWithTimeout(query, howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite, log);
							allDurations.add(duration);
						} catch (Exception e) {
							failed = true;
							failureMessage = e.toString();
						}
					}
					if (failed)
						throw new Exception(failureMessage);
				}
				
				for (String qplan : allQueryPlans) {
					if (!qplan.toLowerCase().equals(theFirstQueryPlan.toLowerCase()))
						log.error("Warning: diff query plans: " + qplan + " and " + theFirstQueryPlan);
				}

				if (useExplainInsteadOfRunningQueries)
					performanceValue = new PerformanceValue(theFirstQueryPlan, allOptimizerCosts);
				else
					performanceValue = new PerformanceValue(theFirstQueryPlan, allDurations, allOptimizerCosts);
					
				secondsSpentMeasuringQueryLatency += t2.lapSeconds();
				++ numberOfCasesWithActualMeasuring;
			}
			
			if (experimentCache!=null)
				experimentCache.cachePerformance(query, new PhysicalDesign(physicalStructuresToInclude), needsAccurate, performanceValue);
		}

		performanceRecord.record(designAlgorithmName, new PerformanceValueWithDesign(performanceValue, new PhysicalDesign(physicalStructuresToInclude)));
	}
	
	protected abstract PlanEstimate getQueryPlanAndCostEstimate(String query, List<PhysicalStructure> physicalStructuresToInclude) throws Exception;
		
	protected abstract String getQueryPlan(String query, List<PhysicalStructure> physicalStructuresToInclude) throws Exception;

	protected Map<PhysicalStructure, DeployedPhysicalStructure> getProjectionStructureMap() throws Exception {
		Set<DeployedPhysicalStructure> currentProjections = dbDeployment.getCurrentlyDeployedStructures();
		Map<PhysicalStructure, DeployedPhysicalStructure> structureMap = new HashMap<PhysicalStructure, DeployedPhysicalStructure>();
		for (DeployedPhysicalStructure p : currentProjections)
			structureMap.put(p.getStructure(), p);
		return structureMap;

	}

	public abstract boolean thisQueryUsesAtLeastOneOfTheseStructures(String query, List<PhysicalStructure> whichProjectionsToInclude) throws Exception; 
	
	public String reportStatistics() {
		String msg = "minsSpentWaitingForMissingStructuresToBeDeployed=" + secondsSpentWaitingForMissingStructuresToBeDeployed /60 +
					", minsSpentMeasuringQueryLatency=" + secondsSpentMeasuringQueryLatency/60 + 
					", minsSpentProducingQueryPlans=" + secondsSpentProducingQueryPlans/60 + 
					", minsSpentEmptyingCache=" + secondsSpentEmptyingCache/60 + " (emptied cach " + numberOfCacheEmptying + " times)\n";
		long total = numberOfCasesIdenticalWithPreviousCases + numberOfCasesWithPreviouslySeenPlan + numberOfCasesWithActualMeasuring + 1; // we add 1 to avoid division by zero
		msg = msg + "numberOfCasesIdenticalWithPreviousCases=" + numberOfCasesIdenticalWithPreviousCases + " (" + 100*numberOfCasesIdenticalWithPreviousCases/total + " %), " +
					"numberOfCasesWithPreviouslySeenPlan="+ numberOfCasesWithPreviouslySeenPlan + " (" + 100*numberOfCasesWithPreviouslySeenPlan/total + " %), " +
					"numberOfCasesWithActualMeasuring="+numberOfCasesWithActualMeasuring + " (" + 100*numberOfCasesWithActualMeasuring/total + " %)\n";
		return msg;
	}
 	
	
	/*
	 * If the returned result set is null, it means that the query did not finish within the given time bound.
	 * Also, the caller is responsible for closing the ResultSet and the two statements
	 */
	public ResultSet evaluateQueryWithTimeout(String sql, long timeoutInSeconds, Statement primary_stmt, Statement secondary_stmt, BLog log) throws Exception {
		// String sql = "select count(*) from st_etl_2.ident_83 a left outer join st_etl_2.ident_83 b on a.ident_2669 = b.ident_2669 and b.ident_2251 = a.ident_2251";
		
        ExecutorService executor = Executors.newSingleThreadExecutor();
        QueryExecutor queryExecutor = new QueryExecutor(sql, primary_stmt, log);
        Future<ResultSet> future = executor.submit(queryExecutor);
        ResultSet res = null;
        
        try {
        	log.status(LogLevel.DEBUG, "started the following query: " + sql);
            res = future.get(timeoutInSeconds, TimeUnit.SECONDS);
            if (!queryExecutor.ranWithoutError())
            	throw new Exception("Query ran with errors: " + queryExecutor.getErrorMessage());
            
            log.status(LogLevel.DEBUG, "finished this query before the timeout: " + sql);
        } catch (TimeoutException e) {
        	log.status(LogLevel.VERBOSE, "This query: " + sql + " did not finish within the " + timeoutInSeconds + " seconds of time bound");
        	terminateAllRunningQueries(null, secondary_stmt, log);
        }
        
        executor.shutdown();
        
        return res;
	}

	public Long measureQueryLatencyInMilliSecondsWithTimeout(String sql, long timeoutInSeconds, BLog log) throws Exception {
		// String sql = "select count(*) from st_etl_2.ident_83 a left outer join st_etl_2.ident_83 b on a.ident_2669 = b.ident_2669 and b.ident_2251 = a.ident_2251";
		
		Statement primary_stmt = queryConnection.createStatement();
		Statement secondary_stmt = controlConnection.createStatement();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        QueryMeter queryMeter = new QueryMeter(sql, primary_stmt, log);
        Future<Long> future = executor.submit(queryMeter);
        Long latency = null;
        
        try {
        	log.status(LogLevel.DEBUG, "started the following query: " + sql);
            latency = future.get(timeoutInSeconds, TimeUnit.SECONDS);
            if (!queryMeter.ranWithoutError())
            	throw new Exception("Query ran with errors: " + queryMeter.getErrorMessage());
            
            log.status(LogLevel.DEBUG, "finished this query before the timeout: " + sql + " with latency="+ latency + " milliseconds");
        } catch (TimeoutException e) {
        	log.status(LogLevel.VERBOSE, "This query: " + sql + " did not finish within the " + timeoutInSeconds + " seconds of time bound");
        	terminateAllRunningQueries(null, secondary_stmt, log);
        	latency = Long.MAX_VALUE;
        }
        
        executor.shutdown();
        
        return latency;
	}

	
	protected abstract void terminateAllRunningQueries(String dbUser, Statement stmt, BLog log) throws Exception;
	
	public List<Long> measureAvgLatenciesForMultipleClusteredWindows(List<ClusteredWindow> cWindows, List<PhysicalStructure> design, boolean useOnlyFirstQuery, boolean useExplainInsteadOfRunningQueries) throws Exception {
		List<Long> latencies = new ArrayList<Long>();
		for (int i=0; i<cWindows.size(); ++i) {
			latencies.add(measureAvgLatencyForOneClusteredWindow(cWindows.get(i), design, useOnlyFirstQuery, useExplainInsteadOfRunningQueries));
			if (latencies.get(latencies.size()-1)==Long.MAX_VALUE)
				log.status(LogLevel.VERBOSE, "The " + i +"'th ClusteredWindow had an average latency of Infinity");
		}
		return latencies;
	}
	
	/*
	 * return time is in miliseconds or optimizer cost units
	 */
	public long measureAvgLatencyForOneClusteredWindow(ClusteredWindow cWindow, List<PhysicalStructure> design, boolean useOnlyFirstQuery, boolean useExplainInsteadOfRunningQueries) throws Exception {
		long sumLatency = 0;
		for (Cluster c : cWindow.getClusters()) {
			long thisClusterLatency;
			if (useOnlyFirstQuery) {
				PerformanceRecord p = new PerformanceRecord(c.retrieveAQueryAtPosition(0).getSql());
				measureLatency(p, design, "ok", useExplainInsteadOfRunningQueries);
				long oneLatency = (useExplainInsteadOfRunningQueries ?
									p.getPerformanceValueWithDesign("ok").getMeanOptimizerCost() :
									p.getPerformanceValueWithDesign("ok").getMeanActualLatency());
				assert c.getFrequency()>0;
				thisClusterLatency = (oneLatency==Long.MAX_VALUE ? Long.MAX_VALUE : oneLatency * c.getFrequency());
			} else {
				thisClusterLatency = measureSumLatency(c.getAllSql(), design, useExplainInsteadOfRunningQueries);
			}
			sumLatency = (thisClusterLatency == Long.MAX_VALUE || sumLatency == Long.MAX_VALUE  ? Long.MAX_VALUE : sumLatency + thisClusterLatency);
			if (thisClusterLatency == Long.MAX_VALUE)
				log.status(LogLevel.VERBOSE, "Cluster caused infinity: " + c.toString());
		}
		double dsum = (sumLatency == Long.MAX_VALUE ? Double.POSITIVE_INFINITY : (double)sumLatency);
		double avgLatency = dsum / cWindow.totalNumberOfQueries();
		long lAvgLatency = (Double.isInfinite(avgLatency) && avgLatency>0 ? Long.MAX_VALUE : (long)avgLatency);
		
		return lAvgLatency;
	}
	
    protected class PlanEstimate {
    	final String queryPlan;
    	final Long optmizerCostEstimate;
    	public PlanEstimate(String queryPlan, Long optimizerCostEstimate) {
    		this.queryPlan = queryPlan;
    		this.optmizerCostEstimate = optimizerCostEstimate;
    	}
    	public String getQueryPlan() {
    		return queryPlan;
    	}
    	public Long getOptmizerCostEstimate() {
    		return optmizerCostEstimate;
    	}
    }
	
//	public static void main(String[] args) throws Exception {
//		ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile("/tmp/blah.cache");
//		String dbName = "wide";
//		DatabaseLoginConfiguration dbLogin = VerticaConnection.createDefaultDBLoginByNameAndServerAlias(dbName, "real_full_db");
//		DBDeployer dbDeployer = new VerticaDeployer(LogLevel.VERBOSE, dbLogin, experimentCache, false);
//		DatabaseInstance dbDeployment = dbDeployer;
//		LatencyMeter latencyMeter = new VerticaLatencyMeter(LogLevel.VERBOSE, true, dbLogin, new ExperimentCache("/tmp/blah2.cache", 1, 1, 1, new VerticaQueryPlanParser()), 
//				dbDeployment, null, 10*60);
//		
//		PhysicalStructure projStruct = experimentCache.getDeployedPhysicalStructureBaseNamesToStructures().get("proj_5250002");
//		List<PhysicalStructure> includedProjections = new ArrayList<PhysicalStructure>();
//		includedProjections.add(projStruct);
//		String query = "SELECT min(col42) FROM public.wide100 WHERE col42 <= 1154 AND col29 <= 1170 LIMIT 10;";
//		List<String> chosenQueries = new ArrayList<String>();
//		chosenQueries.add(query);
//		boolean isYes = latencyMeter.thisQueryUsesAtLeastOneOfTheseStructures(query, includedProjections);
//		System.out.println("Answer was " + isYes);
//		
//	}

}

/*
 * This class is supposed to be only executing the command as a separate thread. 
 * It is the responsibility of whoever calls this object to ensure that the cache is empty, etc.
 */
class QueryExecutor implements Callable<ResultSet> {
	BLog log = null;
	Statement stmt = null;
	String sql = null;
	
	String error = null;
	Boolean failed = null;
	
	public QueryExecutor(String sql, Statement stmt, BLog log) {
		this.log = log;
		this.stmt = stmt;
		this.sql = sql;
	}
	
	public boolean ranWithoutError() throws Exception {
		if (failed == null)
			throw new Exception("You have not excecuted anything yet!");
		else
			return !failed;
	}
	
	public String getErrorMessage() {
		return error;
	}
	
    @Override
    public ResultSet call() throws Exception {
    	ResultSet res = null;
    	// Long duration;
		try {
			res = stmt.executeQuery(sql);
			failed = false;
		} catch (Exception e) {
			error = e.getMessage();	
			failed = true;
		}

		/*
		log = null;
		stmt = null;
		sql = null;		
		 */
		
		return res;
    }
}

class QueryMeter implements Callable<Long> {
	BLog log = null;
	Statement stmt = null;
	String sql = null;
	
	String error = null;
	Boolean failed = null;
	
	public QueryMeter (String sql, Statement stmt, BLog log) {
		this.log = log;
		this.stmt = stmt;
		this.sql = sql;
	}
	
	public boolean ranWithoutError() throws Exception {
		if (failed == null)
			throw new Exception("You have not excecuted anything yet!");
		else
			return !failed;
	}
	
	public String getErrorMessage() {
		return error;
	}
	
    @Override
    public Long call() throws Exception {
    	Long latency = null;
    	// Long duration;
		try {
			Timer t = new Timer();
			ResultSet res = stmt.executeQuery(sql);
			latency = (long) t.lapMillis();
			res.close();
			failed = false;
		} catch (Exception e) {
			error = e.getMessage();	
			failed = true;
			latency = null;
		}

		/*
		log = null;
		stmt = null;
		sql = null;		
		 */
		
		return latency;
    }
}


/*
class PlanEstimateDuration extends PlanEstimate {
	final Long duration;
	public PlanEstimateDuration(String queryPlan, Long optimizerCostEstimate, Long duration) {
		super(queryPlan, optimizerCostEstimate);
		this.duration = duration;
	}
	public Long getDuration() {
		return duration;
	}
	
	List<String> allQueryPlans = new ArrayList<String>();
	List<Long> allOptimizerCostEstimates = new ArrayList<Long>();
	List<Long> allDurations = new ArrayList<Long>();
	
	public void record(String queryPlan, Long optimizerCostEstimate, Long duration) {
		allQueryPlans.add(queryPlan);
		allOptimizerCostEstimates.add(optimizerCostEstimate);
		allDurations.add(duration);
	}

	public List<String> getAllQueryPlans() {
		return allQueryPlans;
	}

	public List<Long> getAllOptimizerCostEstimates() {
		return allOptimizerCostEstimates;
	}

	public List<Long> getAllDurations() {
		return allDurations;
	}
}
*/
