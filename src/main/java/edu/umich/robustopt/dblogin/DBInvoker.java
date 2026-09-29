package edu.umich.robustopt.dblogin;

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


import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public class DBInvoker {
	protected BLog log;
	protected DatabaseLoginConfiguration databaseLoginConfiguration;
	protected ExperimentCache experimentCache;
	protected Connection dbConnection;

	
	public DBInvoker (LogLevel verbosity, DatabaseLoginConfiguration databaseLogin, ExperimentCache experimentCache) throws Exception {
		this.log = new BLog(verbosity);
		this.databaseLoginConfiguration = databaseLogin;
		this.experimentCache = experimentCache;
		this.dbConnection = databaseLoginConfiguration.createConnection();
	}
	
	public void closeConnection() throws SQLException {
		dbConnection.close();
	}
	
	public Set<String> fetchAllUserSchemas() throws SQLException {
		Set<String> schemas = new HashSet<String>();
		Statement stmt = dbConnection.createStatement();
		ResultSet res = stmt.executeQuery("select schema_name from v_catalog.schemata where not is_system_schema"); 
		int rc = 0;
		while (res.next()) {
			schemas.add(res.getString(1));
			++ rc;
		}
		log.status(LogLevel.DEBUG, "retrieved "+ rc + " schema names: " + schemas);
		res.close();
		stmt.close();
		return schemas;
	}

	public Set<String> fetchAllTables(String schemaName) throws SQLException {
		Set<String> tables = new HashSet<String>();
		Statement stmt = dbConnection.createStatement();
		ResultSet res = stmt.executeQuery("select table_name from v_catalog.all_tables where schema_name = '" + schemaName + "' and table_type = 'TABLE'"); 
		int rc = 0;
		while (res.next()) {
			tables.add(res.getString(1));
			++ rc;
		}
		log.status(LogLevel.DEBUG, "retrieved "+ rc + " table names: " + tables);
		res.close();
		stmt.close();
		return tables;
	}
	
	public Integer getActualNumberOfTuples(String schemaName, String tableName) throws SQLException {
		log.status(LogLevel.VERBOSE, "Computing the number of tuples in " + schemaName + "." + tableName + " ...");
		Integer numerOfTuples = 0;
		Statement stmt = dbConnection.createStatement();
		ResultSet res = stmt.executeQuery("select count(*) from " + schemaName + "." + tableName);
		int rc = 0;
		while (res.next()) {
			String cnt = res.getString(1);
			numerOfTuples += Integer.parseInt(cnt);
			++ rc;
		}
		if (rc!=1)
			log.error("When querying " + schemaName + "." + tableName + " we had " + rc + " rows instead of 1");
		res.close();
		stmt.close();
		
		log.status(LogLevel.VERBOSE, numerOfTuples + " tuples");
		return numerOfTuples;
	}

	public boolean isThisTableEmpty(String schemaName, String tableName) throws SQLException {
		return isThisTableEmpty(dbConnection, log, schemaName, tableName);
	}
	
	public static boolean isThisTableEmpty(Connection dbConnection, BLog log, String schemaName, String tableName) throws SQLException {
		log.status(LogLevel.VERBOSE, "Checking for the emptiness of table " + schemaName + "." + tableName + " ...");
		Statement stmt = dbConnection.createStatement();
		ResultSet res = stmt.executeQuery("select * from " + schemaName + "." + tableName + " limit 1");
		int rc = 0;
		while (res.next()) {
			++ rc;
		}
		
		boolean isEmpty;
		if (rc==0)
			isEmpty = true;
		else if (rc==1)
			isEmpty = false;
		else
			throw new SQLException("When querying " + schemaName + "." + tableName + " we had " + rc + " rows instead of 0 or 1");
		
		res.close();
		stmt.close();
		
		return isEmpty;
	}
	
	
	public Connection getConnection() {
		return dbConnection;
	}
	
}
