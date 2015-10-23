package edu.umich.robustopt.vertica;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.metering.PerformanceValue;
import edu.umich.robustopt.metering.PerformanceValueWithDesign;
import edu.umich.robustopt.metering.ExperimentCache.PerformanceKey;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public class VerticaLatencyMeter extends LatencyMeter {	
//	private transient long secondsSpentWaitingForMissingStructuresToBeDeployed = 0;
//	private transient long secondsSpentMeasuringQueryLatency = 0;
//	private transient long secondsSpentProducingQueryPlans = 0;
//	private transient long secondsSpentEmptyingCache = 0;
//	private transient long numberOfCacheEmptying = 0;
//	private transient long numberOfCasesWithActualMeasuring = 0;
//	private transient long numberOfCasesWithPreviouslySeenPlan = 0;
//	private transient long numberOfCasesIdenticalWithPreviousCases= 0;
	
	public VerticaLatencyMeter (LogLevel verbosity, boolean emptyCacheForEachRun, DatabaseLoginConfiguration databaseLogin, ExperimentCache experimentCache, DatabaseInstance dbDeployment, DBDeployer dbDeployer, long howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite) throws Exception {
		super(verbosity, emptyCacheForEachRun, databaseLogin, experimentCache, dbDeployment, dbDeployer, howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite);
		try {
			this.queryConnection = VerticaConnection.createConnection(databaseLogin);
			this.controlConnection = VerticaConnection.createConnection(databaseLogin);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void emptyCache() throws Exception {
		++numberOfCacheEmptying;
		Timer t = new Timer();
		//empty db's cache
		
		Statement stmt = queryConnection.createStatement();
		ResultSet rs = stmt.executeQuery("select clear_caches();");

		int rc = 0;
		while (rs.next())
		{
			String msg = rs.getString(1);
			String expectedMsg = "Cleared";
			if (!msg.toLowerCase().startsWith(expectedMsg.toLowerCase()))
				throw new Exception("Unknown message when executing select clear_caches(). Msg=" + msg);
			++rc;
		}
		if (rc!=1)
			throw new Exception("emptying cache failed, number of rows returned=" + rc);

		int nAttempts = 1;
		
		String sql = "select barzan_empty_cache()";

		boolean failed = false;
		while (nAttempts <= 2) {
			int status = -1;
			rc = 0;
			try {
				rs = stmt.executeQuery(sql);
				log.status(LogLevel.STATUS, "Using academic version, barzan_empty_cache() has been called");
			} catch (SQLException e) {
				break;
			}
			while (rs.next()) {
				status = rs.getInt(1);
				++rc;
			}

			if (rc!=1 || status != 0) {
				log.error("We could not empty the system's cache, but we're going to try again. Attempt number: " + nAttempts + " rc=" + rc + " status="+status);
				failed = true;
			} else {
				failed = false;
				break;
			}
			++nAttempts;
		}

		log.status(LogLevel.DEBUG, "Successfully emptied the cache.");
		secondsSpentEmptyingCache += t.lapSeconds();
		
		if (failed) 
			throw new Exception("We could not successfully restart the cache: " + sql);

	}


	/**
	 * return time is in miliseconds
	 * @param query
	 * @param whichStructuresToInclude
	 * @return
	 * @throws SQLException
	 */
	public void emptyCacheOld() throws Exception {
		++numberOfCacheEmptying;
		Timer t = new Timer();
		//empty db's cache
		
		Statement stmt = queryConnection.createStatement();
		ResultSet rs = stmt.executeQuery("select clear_caches();");

		int rc = 0;
		boolean cleared = false;
		while (rs.next())
		{
			String msg = rs.getString(1);
			String expectedMsg = "Cleared";
			if (msg.toLowerCase().startsWith(expectedMsg.toLowerCase()))
				cleared = true;
			else
				throw new Exception("Unknown message when executing select clear_caches(). Msg=" + msg);
			++rc;
		}
		if (rc!=1)
			throw new Exception("emptying cache failed, number of rows returned=" + rc);

		//empty system's cache
		String userName = System.getProperty("user.name");
		userName = (userName.equals("dbadmin") ? "dbadmin" : "barzan");
		String command = "ssh -t -t " + userName + "@" + databaseLogin.getDBhost() + " '/home/barzan/restartDB'";
		//String command = "/home/barzan/restartDB";
		int nAttempts = 1;
		
		boolean failed = false;
		while (nAttempts <= 2) {
			Process p = Runtime.getRuntime().exec(command);
			int status = p.waitFor();
			if (status!=0) {
				log.error("We could not empty the system's cache, but we're going to try again. Attempt number: " + nAttempts);
				failed = true;
			} else {
				failed = false;
				break;
			}
			++nAttempts;
		}
		/*
		BufferedReader reader = 
		new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = reader.readLine();
		StringBuilder allLines = new StringBuilder();
		while (line != null) {
			allLines.append(line);
			if (line.toLowerCase().endsWith("started successfully")) {
				succeeded = true;
				break;
			}
			line = reader.readLine();
		}
		
		reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		line = reader.readLine();
		StringBuilder allErrorLines = new StringBuilder();
		while (line != null) {
			allErrorLines.append(line);
			line = reader.readLine();
		}
		*/
		//dbConnection = VerticaConnection.createDefaultConnectionByNameAndServerAlias(DBname, serverAlias);
		log.status(LogLevel.DEBUG, "Successfully emptied the cache.");
		secondsSpentEmptyingCache += t.lapSeconds();
		
		if (failed) 
			throw new Exception("We could not successfully restart the cache: " + command);
		/*
		if (!succeeded)
			throw new Exception("We could not restart the Vertica properly. Here's the output: " + allLines.toString() + " and here's the error: " + allErrorLines.toString());
			//throw new Exception("We could not restart the Vertica properly. And there's  the output: " + allLines.toString() + " and here's the error: " + allErrorLines.toString());
		*/
	}
	
	protected PlanEstimate getQueryPlanAndCostEstimate(String query, List<PhysicalStructure> physicalStructuresToInclude) throws Exception {
		Set<NamedIdentifier> excludeList = whatProjectionsToExclude(physicalStructuresToInclude);
		
		StringBuilder directive = new StringBuilder();
		directive.append("AvoidUsingProjections=");
		directive.append(StringUtils.Join(StringUtils.ElemStringify(new ArrayList<NamedIdentifier>(excludeList)), ","));
		directive.append("");
		
		Statement stmt = queryConnection.createStatement();
		log.status(LogLevel.DEBUG, "set_optimizer_directive: " + directive);
		stmt.execute("select set_optimizer_directives('"+directive+"');");

		ResultSet rs = stmt.executeQuery("explain " + query);
		StringBuilder queryPlan = new StringBuilder();		
		while (rs.next())
			queryPlan.append(rs.getString(1) + "\n");
		long optimizerCostEstimate = dbDeployer.getQueryPlanParser().extractTotalCostsFromRawPlan(queryPlan.toString());
		
		// Let query optimizer use all projections again  
		stmt.execute("select set_optimizer_directives('AvoidUsingProjections=');");
		rs.close();
		stmt.close();
		return new PlanEstimate(queryPlan.toString(), optimizerCostEstimate);
	}

	protected String getQueryPlan(String query, List<PhysicalStructure> physicalStructuresToInclude) throws Exception {
		Set<NamedIdentifier> excludeList = whatProjectionsToExclude(physicalStructuresToInclude);
		
		StringBuilder directive = new StringBuilder();
		directive.append("AvoidUsingProjections=");
		directive.append(StringUtils.Join(StringUtils.ElemStringify(new ArrayList<NamedIdentifier>(excludeList)), ","));
		directive.append("");
		
		Statement stmt = queryConnection.createStatement();
		log.status(LogLevel.DEBUG, "set_optimizer_directive: " + directive);
		stmt.execute("select set_optimizer_directives('"+directive+"');");

		ResultSet rs = null;
		boolean successfullyExecuted = false;
		int iter = 1;
		while (!successfullyExecuted) {
			try {
				rs = stmt.executeQuery("explain " + query);
				successfullyExecuted = true;
			} catch (Exception e) {
				String errMessage = e.getMessage();
				//Invalid input syntax for integer: "EsE"
				Pattern projectionNamePattern = Pattern.compile("Invalid input syntax for integer: \"([^\"]*)\"");
				Matcher m = projectionNamePattern.matcher(errMessage);
				if (m.find() && false) {
					String thingToLookFor = "'" + m.group(1) + "'";
					query = query.replaceAll(thingToLookFor, "1234");
					log.error("bad query execution: iter=" + iter + " We replaced <" + thingToLookFor +"> with " + 1234 + "\nError was: " + errMessage + " new query is " + query);
					// we will just re-try
				} else {
					log.error("we could not execute or even repair this query: " + query);
					throw e;
				}
			}
		}
	
		StringBuilder queryPlan = new StringBuilder();		
		while (rs.next())
			queryPlan.append(rs.getString(1) + "\n");
		
		// Let query optimizer use all projections again  
		stmt.execute("select set_optimizer_directives('AvoidUsingProjections=');");
		
		rs.close();
		stmt.close();
		return queryPlan.toString();
	}
		
	// vertica makes life hard...
	// returns identified to the node-specific projection name (not the logical name)
	// this is because AvoidUsingProjections requires node-specific names
	private Set<NamedIdentifier> whatProjectionsToExclude(List<PhysicalStructure> whichProjectionsToInclude) throws Exception { 
		Map<PhysicalStructure, DeployedPhysicalStructure> structureMap = getProjectionStructureMap();
		Set<PhysicalStructure> missingProjectionStructures = new HashSet<PhysicalStructure>();
		for (PhysicalStructure vproj : whichProjectionsToInclude)
			if (!structureMap.containsKey(vproj))
				missingProjectionStructures.add(vproj);
		
		if (!missingProjectionStructures.isEmpty())
		 	if (dbDeployer==null)
				throw new IllegalArgumentException(missingProjectionStructures.size() + " of the structures are not deployed and you have not provided any deployers: " + missingProjectionStructures);
		 	else {
		 		Timer t = new Timer();
		 		dbDeployer.deployDesign(new ArrayList<PhysicalStructure>(missingProjectionStructures), false);
		 		secondsSpentWaitingForMissingStructuresToBeDeployed += t.lapSeconds();
		 	}
		Set<NamedIdentifier> includeList = new HashSet<NamedIdentifier>();
		structureMap = getProjectionStructureMap();
		for (PhysicalStructure vproj : whichProjectionsToInclude) {
			DeployedPhysicalStructure p = structureMap.get(vproj);
			if (p == null)
				throw new IllegalArgumentException("Somehow you still have a structure that is not deployed (even though we tried): " + vproj);
			includeList.add(p.getStructureIdent());
		}
		
		Set<DeployedPhysicalStructure> currentProjections = dbDeployment.getCurrentlyDeployedStructures();

		Set<NamedIdentifier> excludeList = new HashSet<NamedIdentifier>();
		for (DeployedPhysicalStructure p : currentProjections) {
			NamedIdentifier logicalName = p.getStructureIdent();
			NamedIdentifier nodeName = new NamedIdentifier(p.getSchema(), p.getName());
			if (!includeList.contains(logicalName))
				excludeList.add(nodeName);
		}

		return excludeList;		    
	}
	
	public boolean thisQueryUsesAtLeastOneOfTheseStructures(String query, List<PhysicalStructure> whichProjectionsToInclude) throws Exception {
		List<String> projectionNamesUsedInThePlan;
		String rawExplainOutput;
		if (experimentCache!=null && experimentCache.getExplain(query, new PhysicalDesign(whichProjectionsToInclude)) != null) {
			rawExplainOutput = experimentCache.getExplain(query, new PhysicalDesign(whichProjectionsToInclude));
			projectionNamesUsedInThePlan = dbDeployer.getQueryPlanParser().searchForPhysicalStructureNamesInRawExplainOutput(rawExplainOutput);
			log.status(LogLevel.DEBUG, "query plan with this allowed projections fetched from cache.");			
		} else { // we need to measure those!
			//System.out.println("performance record was NOT found in the cache.");		
			Set<NamedIdentifier> excludeList = whatProjectionsToExclude(whichProjectionsToInclude);
	
			// first, get the query plan!
			StringBuilder directive = new StringBuilder();
			directive.append("AvoidUsingProjections=");
			directive.append(StringUtils.Join(StringUtils.ElemStringify(new ArrayList<NamedIdentifier>(excludeList)), ","));
			directive.append("");
			
			Statement stmt = queryConnection.createStatement();
			log.status(LogLevel.DEBUG, "set_optimizer_directive: " + directive);
			stmt.execute("select set_optimizer_directives('"+directive+"');");
	
			ResultSet rs = stmt.executeQuery("explain " + query);
			StringBuilder queryPlanBuffer = new StringBuilder();
			while (rs.next())
			{
				queryPlanBuffer.append(rs.getString(1) + "\n");
			}
			rs.close();
			stmt.close();
			rawExplainOutput = queryPlanBuffer.toString();
			projectionNamesUsedInThePlan = dbDeployer.getQueryPlanParser().searchForPhysicalStructureNamesInRawExplainOutput(rawExplainOutput);
			
			experimentCache.cacheExplain(query, new PhysicalDesign(whichProjectionsToInclude), rawExplainOutput);
		}
		List<String> whichProjectionNamesToInclude = new ArrayList<String>();
		Map<PhysicalStructure, DeployedPhysicalStructure> structToDeployment = getProjectionStructureMap();
		for (PhysicalStructure p : whichProjectionsToInclude) {
			DeployedPhysicalStructure deployed = structToDeployment.get(p);
			whichProjectionNamesToInclude.add(deployed.getName());
		}
		
		Set<String> allowedProjNamesSet = new HashSet<String>(whichProjectionNamesToInclude);
		if (allowedProjNamesSet.size() != whichProjectionNamesToInclude.size())
			throw new Exception("There were duplicate names");
		int originalSize = allowedProjNamesSet.size();
		allowedProjNamesSet.removeAll(projectionNamesUsedInThePlan);
		int numberOfUnusedProjections = allowedProjNamesSet.size();
		
		if (originalSize != numberOfUnusedProjections) {
			log.status(LogLevel.DEBUG, (originalSize - numberOfUnusedProjections) + " out of " + originalSize + " projections were used for query " + query); 
			return true;
		} else {
			log.status(LogLevel.DEBUG, "None of the " + originalSize + " projections were used for query " + query);
			log.status(LogLevel.DEBUG, "allowed projections were " + 
					StringUtils.Join(StringUtils.ElemStringify(new ArrayList<String>(whichProjectionNamesToInclude)), ",") + " ");
			log.status(LogLevel.DEBUG, "used projections were " + 
					StringUtils.Join(StringUtils.ElemStringify(new ArrayList<String>(projectionNamesUsedInThePlan)), ",") + " ");
			log.status(LogLevel.DEBUG, "query plan was " + dbDeployer.getQueryPlanParser().extractCanonicalQueryPlan(rawExplainOutput));
			return false;
		}		
	}
	
	public String reportStatistics() {
		String msg = "secondsSpentWaitingForMissingStructuresToBeDeployed=" + secondsSpentWaitingForMissingStructuresToBeDeployed /60 +
					", minsSpentMeasuringQueryLatency=" + secondsSpentMeasuringQueryLatency/60 + 
					", minsSpentProducingQueryPlans=" + secondsSpentProducingQueryPlans/60 + 
					", minsSpentEmptyingCache=" + secondsSpentEmptyingCache/60 + " (emptied cach " + numberOfCacheEmptying + " times)\n";
		long total = numberOfCasesIdenticalWithPreviousCases + numberOfCasesWithPreviouslySeenPlan + numberOfCasesWithActualMeasuring + 1; // we add 1 to avoid division by zero
		msg = msg + "numberOfCasesIdenticalWithPreviousCases=" + numberOfCasesIdenticalWithPreviousCases + " (" + 100*numberOfCasesIdenticalWithPreviousCases/total + " %), " +
					"numberOfCasesWithPreviouslySeenPlan="+ numberOfCasesWithPreviouslySeenPlan + " (" + 100*numberOfCasesWithPreviouslySeenPlan/total + " %), " +
					"numberOfCasesWithActualMeasuring="+numberOfCasesWithActualMeasuring + " (" + 100*numberOfCasesWithActualMeasuring/total + " %)\n";
		return msg;
	}
 	
	
	
	protected void terminateAllRunningQueries(String dbUser, Statement stmt, BLog log) throws Exception {
		String sql = "select session_id, statement_id from v_monitor.query_requests where is_executing" + (dbUser==null? "" : " and user_name='" + dbUser + "'");
		
		List<Pair<String, Integer>> sesstion_statements = new ArrayList<Pair<String, Integer>>();
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			String sessionId = rs.getString(1);
			Integer statementId = rs.getInt(2);
			sesstion_statements.add(new Pair<String, Integer>(sessionId, statementId));
		}
		rs.close();
		
		log.status(LogLevel.VERBOSE, "There are " + sesstion_statements.size() + " queries running ...");
		
		// now going to end those queries!
		for (Pair<String, Integer> pair : sesstion_statements) {
			String termSql = "select INTERRUPT_STATEMENT('" + pair.first + "', " + pair.second+")";
			rs = stmt.executeQuery(termSql);
			int rc = 0;
			while (rs.next())
				++rc;
			if (rc!=1)
				throw new Exception("this query didn't work: " + termSql + " as it returned " + rc);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		ExperimentCache experimentCache = ExperimentCache.loadCacheFromFile("/tmp/blah.cache");
		String dbName = "wide";
		DatabaseLoginConfiguration dbLogin = VerticaConnection.createDefaultDBLoginByNameAndServerAlias(dbName, "real_full_db");
		DBDeployer dbDeployer = new VerticaDeployer(LogLevel.VERBOSE, dbLogin, experimentCache, false);
		DatabaseInstance dbDeployment = dbDeployer;
		VerticaLatencyMeter latencyMeter = new VerticaLatencyMeter(LogLevel.VERBOSE, true, dbLogin, new ExperimentCache("/tmp/blah2.cache", 1, 1, 1, new VerticaQueryPlanParser()), dbDeployment, null, 10*60);
		
		PhysicalStructure projStruct = (VerticaProjectionStructure) experimentCache.getDeployedPhysicalStructureBaseNamesToStructures().get("proj_5250002");
		List<PhysicalStructure> includedProjections = new ArrayList<PhysicalStructure>();
		includedProjections.add(projStruct);
		String query = "SELECT min(col42) FROM public.wide100 WHERE col42 <= 1154 AND col29 <= 1170 LIMIT 10;";
		List<String> chosenQueries = new ArrayList<String>();
		chosenQueries.add(query);
		boolean isYes = latencyMeter.thisQueryUsesAtLeastOneOfTheseStructures(query, includedProjections);
		System.out.println("Answer was " + isYes);
	}
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
