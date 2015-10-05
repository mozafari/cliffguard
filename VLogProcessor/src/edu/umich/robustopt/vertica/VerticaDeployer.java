package edu.umich.robustopt.vertica;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.OutputStream;
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


import com.relationalcloud.tsqlparser.schema.Table;
import com.vertica.jdbc.VerticaStatement;


import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.common.RecordedStatement;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public class VerticaDeployer extends DBDeployer {
	private static final String OUR_PROJECTION_SCHEMA = "robustopt";
	private static final String OUR_PROJECTION_NAME_SEQ = "projection_name_table";
	
	private transient DatabaseLoginConfiguration databaseLoginConfiguration;
	private Set<DeployedPhysicalStructure> currentDeployedStructs = null;
	private boolean deployMissingStructsDuringInitialization = false;

	// book keeping
	private transient long secondsSpentInitializing = 0;
	private transient long secondsSpentDeploying = 0;
	private static transient double secondsSpentRetrievingDiskSize = 0;
	private static transient long numberOfProjectionsDeployed = 0;
	
	public VerticaDeployer (LogLevel verbosity, DatabaseLoginConfiguration databaseLoginConfiguration, ExperimentCache experimentCache, boolean deployMissingStructuresDuringInitialization) throws Exception {
		super(verbosity, databaseLoginConfiguration, experimentCache, deployMissingStructuresDuringInitialization);
		this.databaseLoginConfiguration = databaseLoginConfiguration;
		this.deployMissingStructsDuringInitialization = deployMissingStructuresDuringInitialization;
	}
		
	public boolean copyStatistics(DatabaseLoginConfiguration emptyDB, String localPath) {
		String fullUser = databaseLoginConfiguration.getDBuser();
		String fullHost = databaseLoginConfiguration.getDBhost();
		String emptyUser = emptyDB.getDBuser();
		String emptyHost = emptyDB.getDBhost();
		
		try {
			String checkIfStatsHaveBeenAlreadyExported = "/usr/bin/rsh -l " + fullUser + " " + fullHost + " ls " + localPath;
			Process child = Runtime.getRuntime().exec(checkIfStatsHaveBeenAlreadyExported);
			int status = child.waitFor();
			
			if (status==0) {
				log.status(LogLevel.STATUS, "Stats file already exists");
				return true;
			} else 
				log.status(LogLevel.STATUS, "Stats file DOES not already exist");
			
			if (!exportStatistics(localPath)) {
				log.error("Could not export statistics to " + localPath);
				return false;
			}
			
			// Now that we exported the statistics, let us copy it over to the empty DB!
			String cmd = "/usr/bin/scp " + fullUser + "@" + fullHost + ":" + localPath + " " + emptyUser + "@" + emptyHost + ":" + localPath;  
		    child = Runtime.getRuntime().exec(cmd);
			status = child.waitFor();
			
			if (status!=0) {
				log.error("We could not scp the stats file. The following command failed:\n" + cmd);
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}	 
		
		return true;
	}
	
	public Set<String> retrieveAllDeployedStructuresBaseNamesFromDB(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet res;
		String listSql = "select projection_basename from v_catalog.projections where projection_schema = '" + OUR_PROJECTION_SCHEMA + "'";
		res = stmt.executeQuery(listSql);
		
		List<String> allDeployedStructureBaseNamesInTheDatabase = new ArrayList<String>();
		while (res.next())
			allDeployedStructureBaseNamesInTheDatabase.add(res.getString(1));
		res.close();
		stmt.close();
		
		Set<String> allBasenamesSet = new HashSet<String>(allDeployedStructureBaseNamesInTheDatabase);
		
		if (allBasenamesSet.size() != allDeployedStructureBaseNamesInTheDatabase.size())
			System.err.println("our database query returns duplicated projection names.");
		
		return allBasenamesSet;
	}
	
	public Set<DeployedPhysicalStructure> retrieveDeployedStructuresFromDB(Connection conn, 
			Set<String> allDeployedProjectinoBaseNamesInTheDatabase, Set<String> cachedDeployedProjectionsBaseNames) throws Exception {
		
		int skipped = 0;
		int newlyFetched = 0;
		Set<DeployedPhysicalStructure> ourVPs = new HashSet<DeployedPhysicalStructure>();
		
		for (String p : allDeployedProjectinoBaseNamesInTheDatabase) {
			if (cachedDeployedProjectionsBaseNames.contains(p)) {
				++skipped;
				//System.out.print("s");
				continue;
			}		
			VerticaDeployedProjection deployedProj = VerticaDeployedProjection.BuildFrom(conn, OUR_PROJECTION_SCHEMA, p, true);
			Double size_gb = retrieveStructureDiskSizeInGigabytes(conn, deployedProj.getSchema(), deployedProj.getBasename());
			deployedProj.getStructure().setDiskSizeInGigabytes(size_gb);			
			ourVPs.add(deployedProj);
			++newlyFetched;
			if (newlyFetched % (1+allDeployedProjectinoBaseNamesInTheDatabase.size()/100) == 0)
				System.out.print((100*newlyFetched)/(1+allDeployedProjectinoBaseNamesInTheDatabase.size()) + "% ");
		}
		System.out.println("\nskipped " + skipped + " and fetched " + newlyFetched + " deployed projections from DB.");
		return ourVPs;		
	}
	
	public boolean wasCreatedByOurselves(DeployedPhysicalStructure deployedPhysicalStructure) {
		if (deployedPhysicalStructure.getSchema().equals(OUR_PROJECTION_SCHEMA))
			return true;
		else 
			return false;
	}
	
	public boolean dropPhysicalStructure(Connection conn, String schemaName, String projectionName) {
		try {
			Statement stmt = conn.createStatement();
			String dropSql = "drop projection " + schemaName + "." + projectionName;
			int rc = stmt.executeUpdate(dropSql);
			if (rc != 0) {
				log.error("Could not drop the projection. SQL: " + dropSql);
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true; 
	}
	
	public DeployedPhysicalStructure deployStructure(PhysicalStructure structure) throws Exception {
		VerticaProjectionStructure vproj = (VerticaProjectionStructure) structure;
		VerticaDeployedProjection deployedProj = null;
		if (vproj == null) 
			throw new Exception("vproj=null, structure=" + structure + ", type=" + structure.getClass().getCanonicalName());
	
		try {
			Timer t = new Timer();

			RecordedStatement rstmt = new RecordedStatement(dbConnection.createStatement());
			ResultSet res;
			int rc;
			
			Timer outerT = new Timer();
			
			rstmt.execute("CREATE SCHEMA IF NOT EXISTS " + OUR_PROJECTION_SCHEMA, true);
			try {
				// sigh, no CREATE SEQUENCE IF NOT EXISTS 
				rstmt.execute("CREATE SEQUENCE " + OUR_PROJECTION_SCHEMA + "." + OUR_PROJECTION_NAME_SEQ, true);
			} catch (SQLException e) {
				// silently fails if exists (which is what we want)
			}
			
			Double size_gb; 
			Timer innerT = new Timer();
			size_gb = null;
			try {				
				// get new name
				res = rstmt.executeQuery("SELECT NEXTVAL('" + OUR_PROJECTION_SCHEMA + "." + OUR_PROJECTION_NAME_SEQ + "')", false);
				if (!res.next())
					throw new SQLException("Could not get next val in name seq");
				int nextVal = res.getInt(1);
				res.close();
				
				String sql = vproj.createPhysicalStructureSQL(OUR_PROJECTION_SCHEMA + "." + "proj_" + nextVal).get(0);
				rc = rstmt.executeUpdate(sql, true);
				rstmt.execute("select refresh('" + vproj.getProjection_anchor_table().getQualifiedName()+ "');", true);				
				rstmt.execute("select make_ahm_now();", true);
				rstmt.finishDeploy(structure);
				
				size_gb = retrieveStructureDiskSizeInGigabytes(OUR_PROJECTION_SCHEMA, "proj_" + nextVal);
				res.close();
				
				vproj.setDiskSizeInGigabytes(size_gb);
				log.status(LogLevel.DEBUG, "Deploying projection structure took "+ innerT.lapMillis() + " ms and "+ (size_gb==null? "NULL" : size_gb.toString()) + " GB of disk space");
				
				deployedProj = VerticaDeployedProjection.BuildFrom(dbConnection, OUR_PROJECTION_SCHEMA, "proj_" + nextVal, true);
			} catch (Exception e) {
				log.error(e.getLocalizedMessage());
			}
		
			// TODO: don't bother dropping projections for now, it just complicates things
			
//			if (dropOldOnes) {
//				/*
//				dbadmin=> select * from v_catalog.projections;
//				 projection_schema_id |    projection_schema     |   projection_id   |                  projection_name                   |            projection_basename            |     owner_id      | owner_name |  anchor_table_id  |             anchor_table_name             |      node_id      |    node_name    | is_prejoin | created_epoch |    create_type    | verified_fault_tolerance | is_up_to_date | has_statistics | is_segmented 
//				----------------------+--------------------------+-------------------+----------------------------------------------------+-------------------------------------------+-------------------+------------+-------------------+-------------------------------------------+-------------------+-----------------+------------+---------------+-------------------+--------------------------+---------------+----------------+--------------
//				    45035996273704968 | public                   | 45035996273713952 | LINEITEM_super                                     | LINEITEM                                  | 45035996273704962 | dbadmin    | 45035996273713890 | LINEITEM                                  |                 0 |                 | f          |             1 | DELAYED CREATION  |                        0 | t             | f              | t
//				    45035996273715614 | s                        | 45035996273715620 | foo_super                                          | foo                                       | 45035996273704962 | dbadmin    | 45035996273715616 | foo                                       |                 0 |                 | f          |             2 | DELAYED CREATION  |                        0 | t             | f              | t
//				    45035996273715648 | v_dbd_example            | 45035996273715652 | vs_designs_node0001                                | vs_designs                                | 45035996273704962 | dbadmin    | 45035996273715650 | vs_designs                                | 45035996273704972 | v_tpch_node0001 | f          |             3 | CREATE TABLE      |                        0 | t             | f              | f
//				    45035996273715648 | v_dbd_example            | 45035996273715704 | vs_design_tables_node0001                          | vs_design_tables                          | 45035996273704962 | dbadmin    | 45035996273715702 | vs_design_tables                          | 45035996273704972 | v_tpch_node0001 | f          |             3 | CREATE TABLE      |                        0 | t             | f              | f
//				    45035996273715774 | v_dbd_example_designname | 45035996273715780 | vs_design_queries_node0001                         | vs_design_queries                         | 45035996273704962 | dbadmin    | 45035996273715778 | vs_design_queries                         | 45035996273704972 | v_tpch_node0001 | f          |             4 | CREATE TABLE      |                        0 | t             | f              | f
//				    45035996273715774 | v_dbd_example_designname | 45035996273715806 | vs_design_overrides_node0001                       | vs_design_overrides                       | 45035996273704962 | dbadmin    | 45035996273715802 | vs_design_overrides                       | 45035996273704972 | v_tpch_node0001 | f          |             4 | CREATE TABLE      |                        0 | t             | f              | f
//				*/
//				
//				Set<NamedIdentifier> justDeployed = new HashSet<NamedIdentifier>();
//				Set<String> coveringSchemas = new HashSet<String>();
//				for (VerticaProjection vproj : projections) {
//					justDeployed.add(new NamedIdentifier(vproj.getAnchor_table_schema(), vproj.getDeployment_projection_name()));
//					coveringSchemas.add(vproj.getAnchor_table_schema());
//				}
//				
//				String sql = "select * from v_catalog.projections where projection_schema IN (" + 
//						StringUtils.Join(new ArrayList<String>(coveringSchemas), ",") + ")";
//				res = stmt.executeQuery(sql);
//				while (res.next()) {
//					NamedIdentifier ident = new NamedIdentifier(res.getString("projection_schema"), res.getString("projection_name"));
//					if (justDeployed.contains(ident))
//						continue;
//					rc = stmt.executeUpdate("DROP PROJECTION " + ident.getQualifiedName() + " CASCADE;");
//					if (rc!=1)
//						throw new SQLException("Could not run: " + "DROP PROJECTION " +ident.getQualifiedName() + " CASCADE;");
//				}
//				res.close();
//				
//			}
			secondsSpentDeploying += t.lapSeconds();
			
			return deployedProj;
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return null;
		}		
	}
				
	public Double retrieveStructureDiskSizeInGigabytes(Connection conn, String projectionSchema, String projectionBaseName) throws SQLException {
		Timer t = new Timer();
		Statement stmt = conn.createStatement();
		ResultSet res;
		int rc = 0;
		Double size_gb = null;

		String sanitySql = "select count(*) from v_monitor.storage_containers "+
				"where storage_type='ROS' and schema_name='" + projectionSchema + "' and projection_name like '" + projectionBaseName + "_node%'";
		res = stmt.executeQuery(sanitySql);
		if (!res.next())
			throw new SQLException("Could not get the number of projections that matched your projection name with the following query: " + sanitySql);
		else
			rc = res.getInt(1);
		res.close();
		
		if (rc>=1) { // the typical case
			// now run the actual query!
			String sizeDeterminingSql = "select sum(used_bytes) / 1024 / 1024 / 1024 as size_gb from v_monitor.storage_containers "+
					"where storage_type='ROS' and schema_name='" + projectionSchema + "' and projection_name like '" + projectionBaseName + "_node%'";
			res = stmt.executeQuery(sizeDeterminingSql);
			
			int rc2=0;
			while (res.next()) {
				size_gb = res.getDouble(1);
				++ rc2;
			}		
			res.close();
			
			if (rc2!=1)
				throw new SQLException("Could not get projection size with the following query: " + sizeDeterminingSql);
		} else { // namely rc < 1, i.e. rc==0 
			BLog log = new BLog(LogLevel.STATUS);
			Table table = findAnchorTableNameWithItsSchema(conn, projectionSchema, projectionBaseName);
			if (isThisTableEmpty(conn, log, table.getSchemaName(), table.getName()))
				size_gb = 0.0;
			else
				throw new SQLException("Perhaps the projection is not fully deployed yet, as the following query returned " + rc + " projections: " + sanitySql);
		}
		
		secondsSpentRetrievingDiskSize  += t.lapSeconds();
		return size_gb;
	}
	
	private Table findAnchorTableNameWithItsSchema(String projectionSchema, String projectionBaseName) throws SQLException {
		return findAnchorTableNameWithItsSchema(dbConnection, projectionSchema, projectionBaseName);
	}
	
	private static Table findAnchorTableNameWithItsSchema(Connection conn, String projectionSchema, String projectionBaseName) throws SQLException {
		Statement stmt = conn.createStatement();
		String tableId_sql = "select anchor_table_id from v_catalog.projections where projection_schema='" + projectionSchema + "' and projection_basename='" + projectionBaseName + "'";
		ResultSet res = stmt.executeQuery(tableId_sql);
		int rc = 0;
		Long anchorTableId = -1L;
		while (res.next()) {
			anchorTableId = res.getLong(1);
			++ rc;
		}
		res.close();
		if (rc!=1)
			throw new SQLException("The following command returned " + rc + " row(s), while we were expecting exactly 1 row: " + tableId_sql);

		String tableSchemaNameSql = "select table_schema, table_name from v_catalog.tables where table_id =" + anchorTableId;
		res = stmt.executeQuery(tableSchemaNameSql);
		int rc2 = 0;
		String table_schema_name = "Test", table_name = "Test";
		while (res.next()) {
			table_schema_name = res.getString(1); 
			table_name = res.getString(2); 
			++ rc2;
		}
		res.close();
		if (rc2!=1)
			throw new SQLException("The following command returned " + rc2 + "row(s), while we were expecting exactly 1 row: " + tableSchemaNameSql);
		stmt.close();
		
		Table table = new Table(table_schema_name, table_name);
		
		return table;
	}
		
	@Override
	public boolean exportStatistics(String statsFileName) throws SQLException {
//	    select analyze_statistics('');
//  	select export_statistics('/tmp/stats_backup.xml');
		boolean success = true;
		
		Statement stmt = dbConnection.createStatement();
		ResultSet rs = stmt.executeQuery("select analyze_statistics('');");
		int rc = 0;
		while (rs.next()) {
			int outputCode = rs.getInt(1);
			if (outputCode != 0)
				success = false;
			++rc;
		}
		if (rc!=1)
			success = false;
		rs.close();
		
		rs = stmt.executeQuery("select export_statistics('" + statsFileName + "');");
		rc = 0;
		while (rs.next()) {
			String outputMsg = rs.getString(1);
			String expectedMsg = "Statistics exported successfully";
			if (!outputMsg.toLowerCase().startsWith(expectedMsg.toLowerCase())) {
				log.error("Unexpected message when exporting statistics: " + outputMsg + " len=" + outputMsg.length() + ", expecting " + expectedMsg.length() + " characters.");
				success = false;
			}
			++rc;
		}
		if (rc!=1)
			success = false;
		rs.close();

		if (success)
			log.status(LogLevel.DEBUG, "Successfully exported statistics to: " + statsFileName + " (ensure to copy it to your designer server)");
		else 
			log.status(LogLevel.DEBUG, "Could not exported statistics to: " + statsFileName);
	
		return success;
	}
	
	public String reportStatistics() {
		String msg = "mins spent initializing=" + secondsSpentInitializing/60 + 
				", mins spent calculating disk size=" + secondsSpentRetrievingDiskSize/60 + 
				", mins spent deploying=" + secondsSpentDeploying/60 + ", numberOfProjectionsDeployed=" + numberOfProjectionsDeployed + "\n";
		return msg;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: VerticaDeployer originalCacheFile databaseConfigFile updatedCacheFile databaseName");
			return;
		}
		String originalCacheFile = args[0];
		String configFile = args[1];
		String updatedCacheFile = (args.length==3 ? args[2] : null);
		String databaseName = args[3];
		
	}

	@Override
	public QueryPlanParser createQueryPlanParser() {
		return new VerticaQueryPlanParser();
	}

}
