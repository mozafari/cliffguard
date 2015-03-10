package edu.umich.robustopt.microsoft;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.*;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class MicrosoftDeployer extends DBDeployer {

	public static final String DTA_IDENTIFIER = "_dta_index_";

	private Map<String, Double> structureSizeMap = new HashMap<String, Double>();
	private DatabaseLoginConfiguration microsoftDatabaseLogin;

	public MicrosoftDeployer(LogLevel verbosity,
			DatabaseLoginConfiguration databaseLoginConfiguration,
			ExperimentCache experimentCache,
			boolean deployMissingStructuresDuringInitialization)
			throws Exception {
		super(verbosity, databaseLoginConfiguration, experimentCache,
				deployMissingStructuresDuringInitialization);
		this.microsoftDatabaseLogin = databaseLoginConfiguration;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("MicrosoftDeployer Main");

		MicrosoftDatabaseLoginConfiguration login = new MicrosoftDatabaseLoginConfiguration(false, "alias", "10.119.124.75",
				1433, "AdventureWorks", "sa", "asdf1234!", "DY-WINVM");

        try {
        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			Connection conn = DriverManager.getConnection(
					String.format("jdbc:sqlserver://%s:%s;databasename=%s", login.getDBhost(), login.getDBport(), login.getDBname()), 
					login.getDBuser(), 
					login.getDBpasswd());
			MicrosoftDeployer deployer = new MicrosoftDeployer(LogLevel.DEBUG, login, null, false);

			// print all structures
			Set<String> allStructures = deployer.retrieveAllDeployedStructuresBaseNamesFromDB(conn);
			for (String structure : allStructures) {
				System.out.println(structure);
			}
			Set<DeployedPhysicalStructure> structures = deployer.retrieveDeployedStructuresFromDB(conn, allStructures, new HashSet<String>());
			System.out.println("# structures = " + structures.size());

			// try dropping first MicrosoftIndex containing "dta" in the index name then deploy again.
			for (DeployedPhysicalStructure structure : structures) {
				MicrosoftDeployedPhysicalStructure microsoftStructure = (MicrosoftDeployedPhysicalStructure)structure;
				PhysicalStructure ps = microsoftStructure.getStructure();
				if (ps instanceof MicrosoftIndex) {
					MicrosoftIndex index = (MicrosoftIndex)ps;
					if (index.getIndexName().contains("dta")) {
						System.out.println("Dropping " + index.getIndexName() + "...");
						if (deployer.dropPhysicalStructure(conn, index.getSchemaName() + "." + index.getTableName(), index.getIndexName())) {
							System.out.println("Dropping " + index.getIndexName() + " successful.");
						}
						System.out.println("Redeploying " + index.getIndexName() + "...");
						if (index.deploy(conn, true)) {
							System.out.println("Redeploying " + index.getIndexName() + " successful.");
						}
						break;
					}
				}
			}

			// try dropping first MicrosoftIndexedView containing "dta" in the index name then deploy again.
			for (DeployedPhysicalStructure structure : structures) {
				MicrosoftDeployedPhysicalStructure microsoftStructure = (MicrosoftDeployedPhysicalStructure)structure;
				PhysicalStructure ps = microsoftStructure.getStructure();
				if (ps instanceof MicrosoftIndexedView) {
					MicrosoftIndexedView index = (MicrosoftIndexedView)ps;
					if (index.getIndexName().contains("dta")) {
						System.out.println("Dropping " + index.getIndexName() + "...");
						if (deployer.dropPhysicalStructure(conn, index.getSchemaName() + "." + index.getViewName(), index.getIndexName())) {
							System.out.println("Dropping " + index.getIndexName() + " successful.");
						}
						System.out.println("Redeploying " + index.getIndexName() + "...");
						if (index.deploy(conn, true)) {
							System.out.println("Redeploying " + index.getIndexName() + " successful.");
						}
						break;
					}
				}
			}
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean copyStatistics(DatabaseLoginConfiguration emptyDB,
			String localPath) {
		// DY: currently not sure the equivalent functionality for this method in SQL Server,
		// and whether this method is really required.
		return false;
	}

	@Override
	public Set<String> retrieveAllDeployedStructuresBaseNamesFromDB(
			Connection conn) throws SQLException {
		// retrieves current list of index names for now.

		Set<String> indexNames = new HashSet<String>();

		// retrieve indexes on tables
		Statement stmt = conn.createStatement();

		// retrieve only nonclustered indexes.
		String getIndexes = "SELECT ind.name as index_name " +
				"FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id " +
				"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
				"WHERE t.is_ms_shipped = 0 AND ind.type = 2 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0 and " +
				"ind.is_hypothetical = 0;";

		ResultSet rs = stmt.executeQuery(getIndexes);

		while (rs.next()) {
			indexNames.add(rs.getString("index_name"));
		}
		rs.close();

		// retrieve indexes on views (indexed view)
		String getIndexedViews = "SELECT i.name as index_name " +
				"FROM sys.objects o " +
				"INNER JOIN sys.indexes i ON o.object_id = i.object_id " +
				"INNER JOIN sys.views v ON v.object_id = i.object_id " +
				"INNER JOIN sys.schemas s ON s.schema_id = v.schema_id " +
				"WHERE o.type = 'V' and i.type = 1";

		rs = stmt.executeQuery(getIndexedViews);
		
		while (rs.next()) {
			indexNames.add(rs.getString("index_name"));
		}

		stmt.close();
		
		return indexNames;
	}

	@Override
	public Set<DeployedPhysicalStructure> retrieveDeployedStructuresFromDB(
			Connection conn,
			Set<String> allDeployedStructureBaseNamesInTheDatabase,
			Set<String> cachedDeployedStructureBaseNames) throws Exception {
		// DY: I am not sure this works as intended? due to the difference in implementation between SQL Server and Vertica.

		Set<DeployedPhysicalStructure> deployedStructures = new HashSet<DeployedPhysicalStructure>();
		int skipped = 0;
		int newlyFetched = 0;

		for (String p : allDeployedStructureBaseNamesInTheDatabase) {
			if (cachedDeployedStructureBaseNames.contains(p)) {
				++skipped;
				continue;
			}		

			MicrosoftDeployedPhysicalStructure deployedStructure = MicrosoftDeployedPhysicalStructure.getDeployedDesignedStructureByNameFromDB(conn, p);
			double size_gb = retrieveStructureDiskSizeInGigabytes(conn, "", p, deployedStructure);
			deployedStructure.getStructure().setDiskSizeInGigabytes(size_gb);
			deployedStructures.add(deployedStructure);
			++newlyFetched;
//			if (newlyFetched % (1+allDeployedStructureBaseNamesInTheDatabase.size()/100) == 0)
//				System.out.print((100*newlyFetched)/(1+allDeployedStructureBaseNamesInTheDatabase.size()) + "% ");
		}
		
		// DY: this output is misleading.. since we call this function outside syncDB..
//		System.out.println("\nskipped " + skipped + " and fetched " + newlyFetched + " deployed projections from DB.");
		return deployedStructures;
	}

	@Override
	public boolean wasCreatedByOurselves(
			DeployedPhysicalStructure deployedPhysicalStructure) {
		// DY: currently we do not differentiate physical structures that DTA created from others in SQL Server.
		// since index names from DTA has "dta" in it, maybe we should differentiate indexes by checking "dta" in the name?
		return deployedPhysicalStructure.getName().contains(DTA_IDENTIFIER);
	}

	@Override
	protected boolean dropPhysicalStructure(Connection conn, String schemaName,
			String structureName) {
		// DY: assuming schemaName here is actually [schemaName].[tableName] for SQL Server.
		try {
			MicrosoftDeployedPhysicalStructure msp = null;
			if (experimentCache != null) {
				msp = (MicrosoftDeployedPhysicalStructure)experimentCache.getDeployedPhysicalStructureNamesToDeployedPhysicalStructures().get(structureName);
			}
			Statement stmt = conn.createStatement();
			String dropSql;
			if (msp != null) {
				dropSql = String.format("DROP INDEX %s ON %s.%s", structureName, msp.getSchema(), msp.getTableOrViewName());
			} else {
				dropSql = String.format("DROP INDEX %s ON %s", structureName, schemaName);
			}
			int rc = stmt.executeUpdate(dropSql);
			if (rc != 0) {
				log.error("Could not drop the index. SQL: " + dropSql);
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	@Override
	public DeployedPhysicalStructure deployStructure(PhysicalStructure structure)
			throws Exception {
		if (!(structure instanceof MicrosoftIndex) && !(structure instanceof MicrosoftIndexedView)) {
			throw new Exception ("Invalid PhysicalStructure object: expected MicrosoftIndex or MicrosoftIndexedView but received: " + 
					structure.getClass().getCanonicalName());
		}

		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

		// DY: should I use dbConnection? create a new connection for now.
		Connection conn = DriverManager.getConnection(
				String.format("jdbc:sqlserver://%s:%s;databasename=%s", microsoftDatabaseLogin.getDBhost(), 
						microsoftDatabaseLogin.getDBport(), microsoftDatabaseLogin.getDBname()), 
						microsoftDatabaseLogin.getDBuser(), 
						microsoftDatabaseLogin.getDBpasswd());

		if (conn != null) {
			log.status(LogLevel.DEBUG, "JDBC connection successful");
		}

		if (structure instanceof MicrosoftIndex) {
			MicrosoftIndex indexToDeploy = (MicrosoftIndex) structure;

	        if (!indexToDeploy.deploy(conn)) {
	        	log.error("Failed to deploy an index: " + indexToDeploy.getIndexName());
	        }
	        retrieveStructureDiskSizeInGigabytes(conn, indexToDeploy.getSchemaName(), indexToDeploy.getIndexName());
	        return new MicrosoftDeployedPhysicalStructure(indexToDeploy.getSchemaName(), indexToDeploy.getIndexName(),
	        		indexToDeploy.getIndexName(), indexToDeploy.getTableName(), indexToDeploy, false);
		} else if (structure instanceof MicrosoftIndexedView) {
			MicrosoftIndexedView indexedViewToDeploy = (MicrosoftIndexedView) structure;

	        if (!indexedViewToDeploy.deploy(conn)) {
	        	log.error("Failed to deploy an index: " + indexedViewToDeploy.getIndexName());
	        }

	        retrieveStructureDiskSizeInGigabytes(conn, indexedViewToDeploy.getSchemaName(), indexedViewToDeploy.getIndexName());
	        return new MicrosoftDeployedPhysicalStructure(indexedViewToDeploy.getSchemaName(), indexedViewToDeploy.getIndexName(),
	        		indexedViewToDeploy.getIndexName(), indexedViewToDeploy.getViewName(), indexedViewToDeploy, false);
		}

		return null;
	}

	@Override
	public Double retrieveStructureDiskSizeInGigabytes(Connection conn,
			String structureSchema, String structureBaseName)
			throws SQLException {
		// structureSchema not required. only use index name (i.e. structureBaseName)

		if (structureSizeMap.containsKey(structureBaseName)) {
			return structureSizeMap.get(structureBaseName);
		}

		MicrosoftDeployedPhysicalStructure deployedStructure;
		boolean isDisabled = false;

		try {
			deployedStructure = MicrosoftDeployedPhysicalStructure.getDeployedDesignedStructureByNameFromDB(conn, structureBaseName);
			if (deployedStructure == null) {
				return 0.0;
			}
			isDisabled = deployedStructure.isDisabled();
			if (isDisabled) {
				deployedStructure.enableStructure(conn);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return 0.0;
		}

		Statement stmt = conn.createStatement();
		String sql = "SELECT SUM(used_page_count) * 8192 as size FROM sys.indexes i " +
				"INNER JOIN sys.dm_db_partition_stats p ON p.object_id = i.object_id AND i.index_id = p.index_id " + 
				"WHERE i.name = '" + structureBaseName + "'";

		ResultSet rs = stmt.executeQuery(sql);

		double size = 0.0;
		double sizeInBytes = 0.0;
		
		if (rs.next()){
			sizeInBytes = rs.getDouble("size");
			size = sizeInBytes / (1024 * 1024 * 1024);
		}
		rs.close();

		if (isDisabled) {
			try {
				deployedStructure.disableStructure(conn);
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}

		structureSizeMap.put(structureBaseName, size);
				
		return size;
	}

	private Double retrieveStructureDiskSizeInGigabytes(Connection conn,
			String structureSchema, String structureBaseName, MicrosoftDeployedPhysicalStructure deployedStructure)
			throws SQLException {

		if (structureSizeMap.containsKey(structureBaseName)) {
			return structureSizeMap.get(structureBaseName);
		}

		boolean isDisabled = false;

		try {
			deployedStructure = MicrosoftDeployedPhysicalStructure.getDeployedDesignedStructureByNameFromDB(conn, structureBaseName);
			if (deployedStructure == null) {
				return 0.0;
			}
			isDisabled = deployedStructure.isDisabled();
			if (isDisabled) {
				deployedStructure.enableStructure(conn);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return 0.0;
		}

		Statement stmt = conn.createStatement();
		String sql = "SELECT SUM(used_page_count) * 8192 as size FROM sys.indexes i " +
				"INNER JOIN sys.dm_db_partition_stats p ON p.object_id = i.object_id AND i.index_id = p.index_id " + 
				"WHERE i.name = '" + structureBaseName + "'";

		ResultSet rs = stmt.executeQuery(sql);

		double size = 0.0;
		double sizeInBytes = 0.0;
		
		if (rs.next()){
			sizeInBytes = rs.getDouble("size");
			size = sizeInBytes / (1024 * 1024 * 1024);
		}
		rs.close();

		if (isDisabled) {
			try {
				deployedStructure.disableStructure(conn);
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}

		structureSizeMap.put(structureBaseName, size);
				
		return size;
	}

	@Override
	public boolean exportStatistics(String statsFileName) throws SQLException {
		// DY: currently not sure the equivalent functionality for this method in SQL Server,
		// and whether this method is really required.
		return false;
	}

	@Override
	public QueryPlanParser createQueryPlanParser() {
		return new MicrosoftQueryPlanParser();
	}

}
