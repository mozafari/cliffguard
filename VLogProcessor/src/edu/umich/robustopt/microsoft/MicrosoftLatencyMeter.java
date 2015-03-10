package edu.umich.robustopt.microsoft;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.LatencyMeter;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.metering.PerformanceValue;
import edu.umich.robustopt.metering.PerformanceValueWithDesign;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.Utils;

public class MicrosoftLatencyMeter extends LatencyMeter {

	private MicrosoftDatabaseLoginConfiguration microsoftLogin;
	private MicrosoftDeployer microsoftDeployer;
	private Connection conn;
	private Connection controlConn;

	public MicrosoftLatencyMeter(LogLevel verbosity,
			boolean emptyCacheForEachRun,
			DatabaseLoginConfiguration databaseLogin,
			ExperimentCache experimentCache, DatabaseInstance dbDeployment,
			DBDeployer dbDeployer,
			long howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite) {
		super(verbosity, emptyCacheForEachRun, databaseLogin, experimentCache,
				dbDeployment, dbDeployer,
				howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite);
		conn = MicrosoftConnection.createConnection(databaseLogin);
		controlConn = MicrosoftConnection.createConnection(databaseLogin);
		try {
			microsoftDeployer = new MicrosoftDeployer(verbosity, databaseLogin, experimentCache, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void emptyCache() throws Exception {
		// DY: performs "CHECKPOINT" then "DBCC DROPCLEANBUFFERS" to clean buffers.
		++numberOfCacheEmptying;
		Timer t = new Timer();
		
		Statement stmt = conn.createStatement();
		String createCheckpoint = "CHECKPOINT";
		String dropCleanBuffers = "DBCC DROPCLEANBUFFERS";
		String dropPlanCache = "DBCC FREEPROCCACHE";

		stmt.execute(createCheckpoint);
		SQLWarning warning = stmt.getWarnings();
		while (warning != null) {
//			System.out.println(warning.getMessage());
			warning = warning.getNextWarning();
		}
		String msg = "";
		stmt.execute(dropPlanCache);
		warning = stmt.getWarnings();
		while (warning != null) {
			msg += warning.getMessage();
			warning = warning.getNextWarning();
		}
		if (!msg.contains("DBCC execution completed.")) {
			log.error("Failed to clear db cache.");
		}
		msg = "";
		stmt.execute(dropCleanBuffers);
		warning = stmt.getWarnings();
		while (warning != null) {
			msg += warning.getMessage();
			warning = warning.getNextWarning();
		}
		if (!msg.contains("DBCC execution completed.")) {
			log.error("Failed to clear db cache.");
		}

		// for now, we just assume the commands ran successfully. (retry will be added after unit testing is done)
		log.status(LogLevel.DEBUG, "Successfully emptied the cache.");
		secondsSpentEmptyingCache += t.lapSeconds();
	}

	@Override
	public void measureLatency(PerformanceRecord performanceRecord,
			List<PhysicalStructure> whichStructuresToInclude,
			String designAlgorithmName, int numberOfRepetitions,
			boolean useExplainInsteadOfRunningQueries) throws Exception {

		assert (numberOfRepetitions>=1);

		String query = performanceRecord.getQuery();		
		PerformanceValue performanceValue;
		boolean needsAccurate = !useExplainInsteadOfRunningQueries;


		if (experimentCache!=null && experimentCache.getPerformance(query, new PhysicalDesign(whichStructuresToInclude), needsAccurate) != null) {
			performanceValue = experimentCache.getPerformance(query, new PhysicalDesign(whichStructuresToInclude), needsAccurate);
			log.status(LogLevel.DEBUG, "performance value with this allowed projections fetched from cache.");
			++numberOfCasesIdenticalWithPreviousCases;
		} else {
			Timer t1 = new Timer();
			
			Set<String> allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
//			Set<String> excludeList = whatStructuresToExclude(whichStructuresToInclude);
			Set<DeployedPhysicalStructure> deployedStructures = microsoftDeployer.retrieveDeployedStructuresFromDB(conn, allDeployedStructureNames, new HashSet<String>());
//			Set<DeployedPhysicalStructure> deployedStructures = microsoftDeployer.getCurrentlyDeployedStructures();
//			Set<MicrosoftDeployedPhysicalStructure> disabledStructures = new HashSet<MicrosoftDeployedPhysicalStructure>();

			Set<DeployedPhysicalStructure> includeStructures = new HashSet<DeployedPhysicalStructure>();
			Set<DeployedPhysicalStructure> excludeStructures = new HashSet<DeployedPhysicalStructure>();
			List<PhysicalStructure> missingStructures = new ArrayList<PhysicalStructure>();

			// DY: I know this is dumb way.. will fix later
			for (PhysicalStructure p : whichStructuresToInclude) {
				boolean isFound = false;
				for (DeployedPhysicalStructure deployed : deployedStructures) {
					if (p.equals(deployed.getStructure())) {
						isFound = true;
						break;
					}
				}
				if (!isFound) {
					missingStructures.add(p);
				}
			}

			if (!missingStructures.isEmpty()) {
				microsoftDeployer.deployDesign(missingStructures, false);
				allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
				deployedStructures = microsoftDeployer.retrieveDeployedStructuresFromDB(conn, allDeployedStructureNames, new HashSet<String>());
			}

			for (DeployedPhysicalStructure p : deployedStructures) {
				if (whichStructuresToInclude.contains(p.getStructure())) {
					includeStructures.add(p);
				} else {
					excludeStructures.add(p);
				}
			}
//
//			if (whichStructuresToInclude.size() != includeStructures.size()) {
//				throw new Exception("# of structures to include are not matched.");
//			}

			log.status(LogLevel.DEBUG, String.format("Measuring latency: # structures to include = %d, exclude = %d, missing = %d, total # of structures = %d", 
					includeStructures.size(), excludeStructures.size(), missingStructures.size(), deployedStructures.size()));

			// disable indexes in the excludeList.
			for (DeployedPhysicalStructure p : includeStructures) {
				MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
				if (msp.isDisabled()) {
					if (!msp.enableStructure(conn)) {
						throw new Exception("Failed to enable a structure: " + msp.getName());
					}
				}
			}
			for (DeployedPhysicalStructure p : excludeStructures) {
				MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
				if (!msp.isDisabled()) {
					if (!msp.disableStructure(conn)) {
						throw new Exception("Failed to enable a structure: " + msp.getName());
					}
				}
			}

//			for (DeployedPhysicalStructure p : deployedStructures) {
//				if (excludeList.contains(p.getName())) {
//					MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
//					if (!msp.isDisabled()) {
//						if (!msp.disableStructure(conn)) {
//							throw new Exception("Failed to disable a structure: " + msp.getName());
//						}
//					}
//				} else {
//					MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
//					if (msp.isDisabled()) {
//						if (!msp.enableStructure(conn)) {
//							throw new Exception("Failed to enable a structure: " + msp.getName());
//						}
//					}
//				}
//			}

			// get first query plan.
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("SET SHOWPLAN_ALL ON");
			ResultSet rs = null;
			try {
				rs = stmt.executeQuery(query);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Query execution error:");
				System.out.println(query);
				throw e;
			}
			StringBuilder queryPlan = new StringBuilder();

			// retrieve query plan.
			while (rs.next()) {
				queryPlan.append(rs.getString(1) + "\n");
			}
			rs.close();
			stmt.executeUpdate("SET SHOWPLAN_ALL OFF");
			stmt.close();
			String theFirstQueryPlan = queryPlan.toString();

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
				
				Statement controlStmt = controlConn.createStatement();
				
				for (int rep = 0; rep < numberOfRepetitions; ++ rep) {
					if (emptyCacheForEachRun) emptyCache();

					stmt = conn.createStatement();
					stmt.executeUpdate("SET SHOWPLAN_ALL ON");
					rs = stmt.executeQuery(query);
					queryPlan = new StringBuilder();

					// retrieve query plan.
					boolean firstRow = true;
					long optimizedCostEstimate = 0;
					while (rs.next()) {
						queryPlan.append(rs.getString(1) + "\n");
						if (firstRow) {
							// this is not in milliseconds or anything. MSDN says it is 'internal unit of measure'.
							// multiplied by 10,000,000 to make it significant.
							optimizedCostEstimate = (long)(rs.getDouble("TotalSubtreeCost") * 10000000);
							firstRow = false;
						}
					}
					allOptimizerCosts.add(optimizedCostEstimate);
					allQueryPlans.add(queryPlan.toString());
					rs.close();
					stmt.executeUpdate("SET SHOWPLAN_ALL OFF");

					boolean failed = false;
					String failureMessage = null;

					if (!useExplainInsteadOfRunningQueries) {
						log.status(LogLevel.DEBUG, "Executing: " + query);
						try {
							long duration = measureQueryLatencyInMilliSecondsWithTimeout(query, howLongToWaitForEachQueryInSecondsBeforeConsideringItInfinite, stmt, controlStmt, log);
							allDurations.add(duration);
						} catch (Exception e) {
							failed = true;
							failureMessage = e.getMessage();
						}
						
//						if (failed) {
//							// rollback disabled indexes.
//							for (MicrosoftDeployedPhysicalStructure msp : disabledStructures) {
//								msp.enableStructure(conn);
//							}
//							throw new Exception(failureMessage);
//						}
					}
				} // end for
				controlStmt.close();

				for (String plan : allQueryPlans) {
					if (!plan.toLowerCase().equals(theFirstQueryPlan.toLowerCase()))
						log.error("Warning: diff query plans: " + plan + " and " + theFirstQueryPlan);
				}

				if (useExplainInsteadOfRunningQueries)
					performanceValue = new PerformanceValue(theFirstQueryPlan, allOptimizerCosts);
				else
					performanceValue = new PerformanceValue(theFirstQueryPlan, allDurations, allOptimizerCosts);
					
				secondsSpentMeasuringQueryLatency += t2.lapSeconds();
				++ numberOfCasesWithActualMeasuring;
			} //end if

			stmt.close();

			if (experimentCache!=null)
				experimentCache.cachePerformance(query, new PhysicalDesign(whichStructuresToInclude), needsAccurate, performanceValue);

			// rollback disabled indexes.
//			for (MicrosoftDeployedPhysicalStructure msp : disabledStructures) {
//				msp.enableStructure(conn);
//			}
		} //end if

		performanceRecord.record(designAlgorithmName, new PerformanceValueWithDesign(performanceValue, new PhysicalDesign(whichStructuresToInclude)));
	}

	private Set<String> whatStructuresToExclude(List<PhysicalStructure> structuresToInclude) throws Exception{

		Set<String> allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
		Set<String> structureNamesToDeploy = new HashSet<String>();
		Set<String> structureNamesToInclude = new HashSet<String>();
		Set<MicrosoftIndex> missingIndexes = new HashSet<MicrosoftIndex>();
		Set<MicrosoftIndexedView> missingIndexedViews = new HashSet<MicrosoftIndexedView>();

		for (PhysicalStructure structureToInclude : structuresToInclude) {
			if (!(structureToInclude instanceof MicrosoftIndex) && !(structureToInclude instanceof MicrosoftIndexedView)) {
				log.error("Expected MicrosoftIndex or MicrosoftIndexedView, but received: " + structureToInclude.getClass().getCanonicalName());
				return null;
			}
			if (structureToInclude instanceof MicrosoftIndex) {
				MicrosoftIndex index = (MicrosoftIndex)structureToInclude;
				structureNamesToInclude.add(index.getIndexName());
				if (!allDeployedStructureNames.contains(index.getIndexName())) {
					missingIndexes.add(index);
					structureNamesToDeploy.add(index.getIndexName());
				}
			} else if (structureToInclude instanceof MicrosoftIndexedView) {
				MicrosoftIndexedView indexedView = (MicrosoftIndexedView)structureToInclude;
				structureNamesToInclude.add(indexedView.getIndexName());
				if (!allDeployedStructureNames.contains(indexedView.getIndexName())) {
					missingIndexedViews.add(indexedView);
					structureNamesToDeploy.add(indexedView.getIndexName());
				}
			}
		}

		if (!missingIndexes.isEmpty()) {
			Timer t = new Timer();
			for (MicrosoftIndex index : missingIndexes) {
				index.deploy(conn);
			}
			secondsSpentWaitingForMissingStructuresToBeDeployed += t.lapSeconds();
		}
		if (!missingIndexedViews.isEmpty()) {
			Timer t = new Timer();
			for (MicrosoftIndexedView view : missingIndexedViews) {
				view.deploy(conn);
			}
			secondsSpentWaitingForMissingStructuresToBeDeployed += t.lapSeconds();
		}

		allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
		for (String structureName : structureNamesToInclude) {
			if (!allDeployedStructureNames.contains(structureName)) {
//				throw new IllegalArgumentException("Somehow you still have a structure that is not deployed (even though we tried): " + structureName);
			}
		}

		Set<String> excludeList = new HashSet<String>();
		for (String deployedName : allDeployedStructureNames) {
			if (!structureNamesToInclude.contains(deployedName)) {
				excludeList.add(deployedName);
			}
		}

		return excludeList;
	}

	private Map<PhysicalStructure, DeployedPhysicalStructure> buildStructureMap(Set<DeployedPhysicalStructure> deployedStructures) throws Exception {
		Map<PhysicalStructure, DeployedPhysicalStructure> structureMap = new HashMap<PhysicalStructure, DeployedPhysicalStructure>();
		for (DeployedPhysicalStructure p : deployedStructures) {
			structureMap.put(p.getStructure(), p);
		}
		return structureMap;
	}



	@Override
	protected String returnQueryPlan(String query, PhysicalDesign physicalDesign)
			throws Exception {

		List<PhysicalStructure> whichStructuresToInclude = 
				Utils.convertPhysicalStructureSetIntoPhysicalList(physicalDesign.getPhysicalStructures());	

		Set<String> allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
		Set<String> excludeList = whatStructuresToExclude(whichStructuresToInclude);
		Set<DeployedPhysicalStructure> deployedStructures = microsoftDeployer.retrieveDeployedStructuresFromDB(conn, allDeployedStructureNames, new HashSet<String>());
		Set<MicrosoftDeployedPhysicalStructure> disabledStructures = new HashSet<MicrosoftDeployedPhysicalStructure>();

		// disable indexes in the excludeList.
		for (DeployedPhysicalStructure p : deployedStructures) {
			if (excludeList.contains(p.getName())) {
				MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
				if (!msp.disableStructure(conn)) {
					throw new Exception("Failed to disable a structure: " + msp.getName());
				}
				disabledStructures.add(msp);
			}
		}

		// get first query plan.
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("SET SHOWPLAN_ALL ON");
		ResultSet rs = stmt.executeQuery(query);
		StringBuilder queryPlan = new StringBuilder();

		// retrieve query plan.
		while (rs.next()) {
			queryPlan.append(rs.getString(1) + "\n");
		}
		rs.close();
		stmt.executeUpdate("SET SHOWPLAN_ALL OFF");
		stmt.close();
		String theFirstQueryPlan = queryPlan.toString();

		// rollback disabled indexes.
		for (MicrosoftDeployedPhysicalStructure msp : disabledStructures) {
			msp.enableStructure(conn);
		}

		return theFirstQueryPlan;
	}

	@Override
	public boolean thisQueryUsesAtLeastOneOfTheseStructures(String query,
			List<PhysicalStructure> whichStructuresToInclude) throws Exception {

		List<String> projectionNamesUsedInThePlan;
		String rawExplainOutput;
		if (experimentCache!=null && experimentCache.getExplain(query, new PhysicalDesign(whichStructuresToInclude)) != null) {
			rawExplainOutput = experimentCache.getExplain(query, new PhysicalDesign(whichStructuresToInclude));
			projectionNamesUsedInThePlan = dbDeployer.getQueryPlanParser().searchForPhysicalStructureNamesInRawExplainOutput(rawExplainOutput);
			log.status(LogLevel.DEBUG, "query plan with this allowed projections fetched from cache.");			
		} else { // we need to measure those!

			Set<String> allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
			Set<DeployedPhysicalStructure> deployedStructures = microsoftDeployer.retrieveDeployedStructuresFromDB(conn, allDeployedStructureNames, new HashSet<String>());

			Set<DeployedPhysicalStructure> includeStructures = new HashSet<DeployedPhysicalStructure>();
			Set<DeployedPhysicalStructure> excludeStructures = new HashSet<DeployedPhysicalStructure>();
			List<PhysicalStructure> missingStructures = new ArrayList<PhysicalStructure>();

			// DY: I know this is dumb way.. will fix later
			for (PhysicalStructure p : whichStructuresToInclude) {
				boolean isFound = false;
				for (DeployedPhysicalStructure deployed : deployedStructures) {
					if (p.equals(deployed.getStructure())) {
						isFound = true;
						break;
					}
				}
				if (!isFound) {
					missingStructures.add(p);
				}
			}

			if (!missingStructures.isEmpty()) {
				microsoftDeployer.deployDesign(missingStructures, false);
				allDeployedStructureNames = microsoftDeployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
				deployedStructures = microsoftDeployer.retrieveDeployedStructuresFromDB(conn, allDeployedStructureNames, new HashSet<String>());
			}

			for (DeployedPhysicalStructure p : deployedStructures) {
				if (whichStructuresToInclude.contains(p.getStructure())) {
					includeStructures.add(p);
				} else {
					excludeStructures.add(p);
				}
			}

			// disable indexes in the excludeList.
			for (DeployedPhysicalStructure p : includeStructures) {
				MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
				if (msp.isDisabled()) {
					if (!msp.enableStructure(conn)) {
						throw new Exception("Failed to enable a structure: " + msp.getName());
					}
				}
			}
			for (DeployedPhysicalStructure p : excludeStructures) {
				MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
				if (!msp.isDisabled()) {
					if (!msp.disableStructure(conn)) {
						throw new Exception("Failed to enable a structure: " + msp.getName());
					}
				}
			}

//			// disable indexes in the excludeList.
//			for (DeployedPhysicalStructure p : deployedStructures) {
//				if (excludeList.contains(p.getName())) {
//					MicrosoftDeployedPhysicalStructure msp = (MicrosoftDeployedPhysicalStructure)p;
//					if (!msp.disableStructure(conn)) {
//						throw new Exception("Failed to disable a structure: " + msp.getName());
//					}
//					disabledStructures.add(msp);
//				}
//			}

			// get first query plan.
			Statement stmt = conn.createStatement();
			stmt.executeUpdate("SET SHOWPLAN_ALL ON");
			ResultSet rs = stmt.executeQuery(query);
			StringBuilder queryPlan = new StringBuilder();

			// retrieve query plan.
			while (rs.next()) {
				queryPlan.append(rs.getString(1) + "\n");
			}
			rs.close();
			stmt.executeUpdate("SET SHOWPLAN_ALL OFF");
			stmt.close();
			rawExplainOutput = queryPlan.toString();
			projectionNamesUsedInThePlan = dbDeployer.getQueryPlanParser().searchForPhysicalStructureNamesInRawExplainOutput(rawExplainOutput);

			// rollback disabled indexes.
//			for (MicrosoftDeployedPhysicalStructure msp : disabledStructures) {
//				msp.enableStructure(conn);
//			}

			int originalSize = allDeployedStructureNames.size();
			allDeployedStructureNames.removeAll(projectionNamesUsedInThePlan);
			int numberOfUnusedProjections = allDeployedStructureNames.size();
			
			if (originalSize != numberOfUnusedProjections) {
				log.status(LogLevel.DEBUG, (originalSize - numberOfUnusedProjections) + " out of " + originalSize + " projections were used for query " + query); 
				return true;
			} else {
				log.status(LogLevel.DEBUG, "None of the " + originalSize + " projections were used for query " + query);
				log.status(LogLevel.DEBUG, "allowed projections were " + 
						StringUtils.Join(StringUtils.ElemStringify(new ArrayList<String>(allDeployedStructureNames)), ",") + " ");
				log.status(LogLevel.DEBUG, "used projections were " + 
						StringUtils.Join(StringUtils.ElemStringify(new ArrayList<String>(projectionNamesUsedInThePlan)), ",") + " ");
				log.status(LogLevel.DEBUG, "query plan was " + rawExplainOutput);
				return false;
			}	
		}
		return false;
	}

	@Override
	protected void terminateAllRunningQueries(String dbUser, Statement stmt,
			BLog log) throws Exception {

		String sql = "SELECT s.session_id FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(sql_handle)" + 
				"INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id" + 
				(dbUser==null ? "" : " WHERE s.login_name='" + dbUser + "'");

		List<Integer> sessionIds = new ArrayList<Integer>();
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			sessionIds.add(rs.getInt("session_id"));
		}
		rs.close();
		
		for (Integer id : sessionIds) {
			String killSql = "KILL " + id.intValue();
			stmt.execute(killSql);
		}
	}

}
