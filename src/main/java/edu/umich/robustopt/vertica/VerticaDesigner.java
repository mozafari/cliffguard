package edu.umich.robustopt.vertica;

import java.io.Serializable;
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


import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public class VerticaDesigner extends DBDesigner {
	
	private String path_to_exported_stats_file;
	private int counter = 0;
	
	//book keeping
	transient private long secondsSpentDesigning = 0;
	transient private long numberOfActualDesigns = 0;

	public VerticaDesigner (LogLevel verbosity, DatabaseLoginConfiguration databaseLogin, String path_to_exported_stats_file, ExperimentCache experimentCache) throws Exception {
		super(verbosity, databaseLogin, experimentCache);
		this.path_to_exported_stats_file = path_to_exported_stats_file;
	}
	
	@Override
	public PhysicalDesign findDesignByWeightedQueries(List<WeightedQuery> weightedQueries, DesignParameters designParameters) throws Exception {
		if (designParameters==null)
			throw new Exception("Invalid DesignParameters: null");
		else if (!(designParameters instanceof VerticaDesignParameters))
			throw new Exception("Invalid DesignParameters: expected VerticaDesignParameters but received " + designParameters.getClass().getCanonicalName() + ", " + designParameters);
		
		VerticaDesignParameters verticaDesignParameters = (VerticaDesignParameters) designParameters;
			
		return findDesign(weightedQueries, true, verticaDesignParameters.designMode);
	}


	private boolean init() {
		try {
			Statement stmt = dbConnection.createStatement();
			
			ResultSet res = stmt.executeQuery("select dbd_create_workspace('example', true)");
			if (!res.next() || res.getInt(1)!=0) 
				throw new SQLException("Could not create workspace");
			else
				log.status(LogLevel.DEBUG, "workspace created successfully.");
				
			res = stmt.executeQuery("select dbd_create_design('example', 'designname')");
			if (!res.next() || res.getInt(1)!=0) 
				throw new SQLException("Could not create design space");
			else
				log.status(LogLevel.DEBUG, "design space created successfully.");

			Set<String> schemas = fetchAllUserSchemas();
			int totalTablesAdded = 0;
			for (String schema : schemas) {
				res = stmt.executeQuery("select dbd_add_design_tables('example', '" + schema +".*')");
				if (!res.next()) 
					throw new SQLException("Could not add this schema to design:"+schema);
				else
					totalTablesAdded += res.getInt(1);
			}
			log.status(LogLevel.VERBOSE, "added "+schemas.size()+" schemas , "+totalTablesAdded+" tables to the design successfully.");
			
			res.close();
			stmt.close();

			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	private boolean reset() {
		try { 
			Statement stmt = dbConnection.createStatement();
			ResultSet resultSet;
			Timer t = new Timer();
			log.status(LogLevel.VERBOSE, "Waiting on previous ongoing designs ...");
			try {
				resultSet = stmt.executeQuery("select dbd_wait_for_deployment('example', 'designname', true);");
			} catch (SQLException e) {
				// We should also return true if design doesn't exist
				return true;
			}
			if (!resultSet.next())
				throw new SQLException("Could not run the dbd_wait_for_deployment function.");
			else if (resultSet.getInt(1)==0)
				log.status(LogLevel.DEBUG, "previous designs finished.");
			else
				log.error("Unrecognizable output form .");
			log.status(LogLevel.VERBOSE, "We spent " + t.lapMinutes() + " minutes waiting for previous designs to finish!");
			
			resultSet = stmt.executeQuery("select dbd_drop_all_workspaces();");
			if (!resultSet.next())
				throw new SQLException("Could not run the clean up process.");
			else if (resultSet.getInt(1)==1)
				log.status(LogLevel.DEBUG, "Cleaned previous DBD deign workspaces.");
			else
				log.status(LogLevel.DEBUG, "No previous DBD deign workspaces to clean.");
			
			resultSet.close();
			stmt.close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	private boolean addDesignQuery(String sql, double weight) {
		if (weight <= 0) 
			throw new IllegalArgumentException("invalid weight");
		try {
			Statement stmt = dbConnection.createStatement();
			sql = sql.replaceAll("'", "''");
			String command = "select dbd_add_design_query('example', 'designname', '" + sql +"', " + weight + ")";
			ResultSet resultSet = stmt.executeQuery(command);
			boolean success;
			if (!resultSet.next()) 
				throw new SQLException("dbd_add_design_query did not return anything for the following query: " + command);
			else if (resultSet.getInt(1)!=1) {
				SQLWarning warning = resultSet.getWarnings();
				throw new SQLException("DBD rejected this query: " + sql, warning);
			} else if (resultSet.next()) {// return more than one row!
				throw new SQLException("DBD returned more than one row for this query: " + sql);
			} else
				success = true;
			
			resultSet.close();
			stmt.close();
			log.status(LogLevel.DEBUG, (success? ("yes(" + weight + "): ") : "no(" + weight + "): ")+ sql);
			return success;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error("no: "+ sql + " error: " + e.getMessage());
			return false;
		}
	}
	
	private boolean addDesignQueriesOneByOne(List<WeightedQuery> weightedQueries) {
		Map<String, Double> counts = new HashMap<String, Double>();
		for (WeightedQuery weightedQuery : weightedQueries) {
			String sql = weightedQuery.query;
			Double weight = weightedQuery.weight;
			Double cur = counts.get(sql);
			if (cur == null)
				cur = 0.0;
			counts.put(sql, cur + weight);
		}
		for (Map.Entry<String, Double> e : counts.entrySet())
			if (!addDesignQuery(e.getKey(), e.getValue()))
				return false;
		return true;
	}
	
	private boolean addDesignQueriesBatchMode(List<String> sqls) {
		log.error("Do not use this function! The dbd_add_design_queries_from_results function is not released yet");
		throw new RuntimeException("Do not use this function! The dbd_add_design_queries_from_results function is not released yet");
//		
//		try {
//			Statement stmt = dbConnection.createStatement();
//			int rc = stmt.executeUpdate("drop table if exists barzan_query_holders");
//			System.out.println("dropping table result:"+rc);
//			rc = stmt.executeUpdate("create table barzan_query_holders(design_query_id int, query_text varchar(65000))");
//			System.out.println("creating table result:"+rc);
//			for (int i=1; i<=sqls.size(); ++i) {
//				String sql = sqls.get(i-1);
//				sql = sql.replaceAll("'", "''");
//				rc = stmt.executeUpdate("insert into barzan_query_holders values (" + i + ", '"+sql+"')");
//				if (rc!=1) {
//					dbConnection.rollback();
//					return false;
//				}
//			}
//			//dbConnection.commit();
//			
//			ResultSet res = stmt.executeQuery("select dbd_add_design_query('example', 'designname', query_text) from barzan_query_holders");
//			System.out.println(res.getInt(1));
//			res.close();
//			stmt.close();
//
//			return true;
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return false;
//		}
	}
	
	private PhysicalDesign findDesign(List<WeightedQuery> weightedQueries, boolean isEmptyDB, VerticaDesignMode designMode) throws Exception {
		Set<WeightedQuery> uniqueWeightedQueries = WeightedQuery.consolidateWeightedQueies(weightedQueries);
		weightedQueries = new ArrayList<WeightedQuery>(uniqueWeightedQueries); // this is a hack so I won't have to rewrite the entire function
		
		if (experimentCache!=null && experimentCache.getDesignByWeight(weightedQueries)!=null){
			log.status(LogLevel.DEBUG, "design loaded from cache instead of DBD");
			return experimentCache.getDesignByWeight(weightedQueries);
		}
		List<VerticaProjectionStructure> projections = new ArrayList<VerticaProjectionStructure>(); 
		if (!reset()) 
			throw new Exception("Could not reset the DBD");
		if (!init())
			throw new Exception("Could not initialize the DBD");
		if (!addDesignQueriesOneByOne(weightedQueries))
			throw new Exception("Could not add the queries to the design space. FAIL.");
		
		try {
			// TODO: implement me
//			if (isEmptyDB && !isDBempty())
//				throw new SQLException("Calling a non-empty DB empty!");

			//if (!designMode.equals("LOAD") && !designMode.equals("QUERY") && !designMode.equals("BALANCED"))
			//	throw new SQLException("Unsupported design mode: "+designMode);
			Timer t = new Timer();
			int startLogIndex = log.getNextIndex();
			
			Statement stmt = dbConnection.createStatement();
			ResultSet res;
			
			if (isEmptyDB) {
				res = stmt.executeQuery("select export_statistics('/tmp/stats_backup.xml');");
				if (!res.next() || !StringUtils.rtrim(res.getString(1)).equals("Statistics exported successfully")) 
					throw new SQLException(
							"Could not export the old stats: "+StringUtils.rtrim(res.getString(1)));
				res.close();
				
				// let's first disable the periodic stats updatating function
				
				res = stmt.executeQuery("select disable_service('TM','AnalyzeRowCount');");
				if (!res.next())
					throw new SQLException("could not run disable_service");
				String msg = StringUtils.rtrim(res.getString(1));
				if (!msg.endsWith("Service disabled"))
					throw new SQLException("Could not disable the AnalyzeRowCount service. Msg:"+msg);
				res.close();
				
				if (path_to_exported_stats_file!=null) {
					res = stmt.executeQuery("select import_statistics('" + path_to_exported_stats_file + "');");
					if (!res.next())
						throw new SQLException("could not run import_statistics");
					msg = StringUtils.rtrim(res.getString(1));
					if (!msg.contains("success")) {
						if (msg.startsWith("WARNING")) {
							log.error("import_statistics received warning - proceeding anyways: " + msg);
						} else {
							throw new SQLException("Could not import the new stats: "+StringUtils.rtrim(res.getString(1)));
						}
					}
					res.close();
				}
			}
			
			log.status(LogLevel.DEBUG, "[INFO] Setting designmode: " + designMode.toString());
			res = stmt.executeQuery("SELECT VERSION()");
			if (!res.next())
				throw new SQLException("Could not get the version of Vertica");
			String versionStr = res.getString(1);
			if (versionStr.contains("v7"))
				// Vertica Version 7
				stmt.execute("select dbd_set_optimization_objective('example', 'designname', '" + designMode.toString() + "')");
			else
				// Vertica Version 6
				stmt.execute("select dbd_set_design_policy('example', 'designname', '" + designMode.toString() + "')");
			
			String command = "select dbd_run_populate_design_and_deploy"+
			  "('example', 'designname',"+
			   "'/tmp/dbd_projections_" + counter + "_" + designMode.toString() + ".sql'," + /* projection definition file */
			   "'/tmp/dbd_deploy_" + (counter++) + "_" + designMode.toString() + ".sql'," +      /*  script to deploy */
			   "False,"+  /* analyze statistics (shouldn't do if you imported statistics) */
			   "False,"+  /* should the projections be deployed? */
			   "False,"+  /* should we clean up by dropping workspace */
			   "False"+   /* continue after error */
			   ");";
			log.status(LogLevel.DEBUG, "design command: " + command);
			res = stmt.executeQuery(command);
			if (!res.next() || res.getInt(1)!=0) 
				throw new SQLException("The output of dbd_run_populate_design_and_deploy was "+res.getInt(1));
			res.close();
			
			// avoid a race condition!
			stmt.execute("select dbd_wait_for_design('example', 'designname');");
			
			boolean finishedWell = false;
			StringBuilder msg = new StringBuilder();
			res = stmt.executeQuery("select * from design_status order by event_time;");
			while (res.next()) {
				msg.append(res.getString("event_time") + " | " + 
							res.getString("design_phase") + " | " + 
							res.getString("phase_step") + "\n");
				if (res.getString("design_phase").equalsIgnoreCase("Design completed successfully")) {
					finishedWell = true;
					break;
				}
			}
			res.close();
			
			if (!finishedWell) 
				throw new SQLException("design failed: \n"+msg.toString());
			
			if (isEmptyDB) {
				res = stmt.executeQuery("select import_statistics('/tmp/stats_backup.xml');");	
				if (!res.next() || (!StringUtils.rtrim(res.getString(1)).equals("Statistics imported successfully") && !StringUtils.rtrim(res.getString(1)).isEmpty())) { 
					String msg2 = res.getString(1);
					log.status(LogLevel.DEBUG, "[WARNING] Could not restore the old stats: "+ msg2.subSequence(0, msg2.indexOf('\n')));
				}
				res.close();
			}
			
			/*
			 * Now fetch the name of the proposed projections
			 */
			stmt.execute("select dbd_wait_for_deployment('example', 'designname', true);");
			String sql = "select * from v_dbd_example.vs_deployment_projections join v_dbd_example.vs_deployment_projection_statements using (deployment_id, deployment_projection_id);";
			log.status(LogLevel.DEBUG, sql);
			res = stmt.executeQuery(sql);
			Connection secondaryConnection;
			boolean createdADifferentConnection = false;
			try {
				secondaryConnection = VerticaConnection.createConnection(databaseLoginConfiguration);
				createdADifferentConnection = true;
			} catch (Exception e) {
				log.status(LogLevel.WARNING, "Sorry, could not create a secondary connection: " + e.getMessage());
				e.printStackTrace();
				secondaryConnection = dbConnection;
			}

			while (res.next()) {
				VerticaDesignProjection vproj = new VerticaDesignProjection(secondaryConnection, res);
				projections.add((VerticaProjectionStructure)vproj.getRepresentation().getStructure());
			}
			res.close();
			if (createdADifferentConnection)
				secondaryConnection.close();
			
			/*
			 * dbadmin=>  select * from v_dbd_example.vs_deployment_projections;
			   deployment_id   | deployment_projection_id | design_name |    deployment_projection_name    | anchor_table_schema | anchor_table_name | deployment_operation | deployment_projection_type | deploy_weight 
			-------------------+--------------------------+-------------+----------------------------------+---------------------+-------------------+----------------------+----------------------------+---------------
			 45035996273712269 |                        1 | designname  | foo_DBD_1_rep_example_designname | s                   | foo               | add                  | DBD                        |           300
			 45035996273712269 |                        2 | designname  | bar_DBD_2_rep_example_designname | s                   | bar               | add                  | DBD                        |             0
			 45035996273712269 |                        3 | N/A         | foo_super                        | s                   | foo               | drop                 | CATALOG                    |             0

			 * 
			 */
				
			
			secondsSpentDesigning += t.lapSeconds();
			String algorithmOutput = log.getMessagesFromIndex(startLogIndex);
			++numberOfActualDesigns;

			VerticaDesign design = new VerticaDesign(projections);
			
			if (experimentCache!=null)
				experimentCache.cacheDesignByWeightedQueries(weightedQueries, design, secondsSpentDesigning, algorithmOutput);
			
			return design;
		} catch (SQLException e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return null;
		}		
	}
	
	@Override
	public String reportStatistics() {
		String msg = "number of actual designs=" + numberOfActualDesigns + ", mins spent designing=" + secondsSpentDesigning/60 + "\n"; 
		return msg;
	}
			
}
