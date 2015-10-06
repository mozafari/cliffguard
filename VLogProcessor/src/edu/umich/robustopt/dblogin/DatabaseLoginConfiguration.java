package edu.umich.robustopt.dblogin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.relationalcloud.tsqlparser.loader.Schema;

import sun.util.logging.resources.logging;

import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;


public abstract class DatabaseLoginConfiguration implements Cloneable {
	public static final boolean safeMode = false;
	
	private boolean empty = false;
	private String DBalias = null;
	
	private String DBhost = null;
	private Integer DBport = 5433;
	private String DBname = null;
	private String DBuser = "dbadmin";
	private String DBpasswd = null;
	
	public DatabaseLoginConfiguration() {
		
	}
	
 	public DatabaseLoginConfiguration(boolean empty, String DBalias, String DBhost, Integer DBport, String DBname, String DBuser, String DBpasswd) {
		this.DBhost = DBhost;
		this.DBport = DBport;
		this.DBname = DBname;
		this.DBuser = DBuser;
		this.DBpasswd = DBpasswd;
	}

	public static List<DatabaseLoginConfiguration> loadDatabaseConfigurations(String filename, String VendorSpecificDatabaseLoginName) {
        BeanFactory factory = new XmlBeanFactory(new FileSystemResource(filename));

        List<DatabaseLoginConfiguration> allServers = (List<DatabaseLoginConfiguration>) factory.getBean("all-servers");
        List<DatabaseLoginConfiguration> allQualifiedServers = new ArrayList<DatabaseLoginConfiguration>();
        
        for (DatabaseLoginConfiguration server : allServers)
        	if (server.getClass().getSimpleName().equals(VendorSpecificDatabaseLoginName))
        		allQualifiedServers.add(server);
        
        return allQualifiedServers;
	}
	
	public static void main(String[] args) throws Exception {
        String filename = "/Users/sina/robust-opt/databases.conf";

        List<DatabaseLoginConfiguration> allServers = loadDatabaseConfigurations(filename, VerticaDatabaseLoginConfiguration.class.getSimpleName());
        System.out.println("You have the following Vertica databases: ");     		
        for (DatabaseLoginConfiguration dbConfig : allServers)
	        System.out.println(dbConfig.toString() + " which has this many tuples in it: " + dbConfig.getTotalNumberOfUserTuples());
        
        allServers = loadDatabaseConfigurations(filename, MicrosoftDatabaseLoginConfiguration.class.getSimpleName());
        System.out.println("You have the following Microsoft databases: ");        		
        for (DatabaseLoginConfiguration dbConfig : allServers)
	        System.out.println(dbConfig.toString() + " which has this many tuples in it: " + dbConfig.getTotalNumberOfUserTuples());
        
        
	}

	public String toString() {
		return (empty ? "{empty" : "{full") + " " + DBname + "@" + DBhost + ":" + DBport + " (user: " + DBuser + ", passwd: " + DBpasswd + ")}";
	}

	public boolean isEmpty() {
		return empty;
	}

	public void setEmpty(boolean empty) {
		this.empty = empty;
	}

	public String getDBalias() {
		return DBalias;
	}

	public void setDBalias(String dBalias) {
		DBalias = dBalias;
	}

	public String getDBhost() {
		return DBhost;
	}

	public void setDBhost(String dBhost) {
		DBhost = dBhost;
	}

	public Integer getDBport() {
		return DBport;
	}

	public void setDBport(Integer dBport) {
		DBport = dBport;
	}

	public String getDBname() {
		return DBname;
	}

	public void setDBname(String dBname) {
		DBname = dBname;
	}

	public String getDBpasswd() {
		return DBpasswd;
	}

	public void setDBpasswd(String dBpasswd) {
		DBpasswd = dBpasswd;
	}

	public String getDBuser() {
		return DBuser;
	}

	public void setDBuser(String dBuser) {
		DBuser = dBuser;
	}
	
	public int getTotalNumberOfUserTuples() throws Exception {
		DBInvoker dbInvoker = new DBInvoker(LogLevel.STATUS, this, null);
		
		System.out.print("Computing total number of tuples in " + toString() + ": ");
		Set<String> allUserSchemas = dbInvoker.fetchAllUserSchemas();
		Set<Pair<String,String>> allSchemaTables = new HashSet<Pair<String,String>>();
		for (String schema : allUserSchemas) {
			 Set<String> tables = dbInvoker.fetchAllTables(schema);
			 for (String table : tables)
				 allSchemaTables.add(new Pair<String, String>(schema, table));
		}
		int totalNumberOfTuples = 0;
		for (Pair<String, String> pair : allSchemaTables) {
			totalNumberOfTuples += dbInvoker.getActualNumberOfTuples(pair.first, pair.second);
			if (totalNumberOfTuples >= calculateFullnessThreshold()) // this is just for efficiency!
				break;
		}
		System.out.println("Total nmber of tuples in " + toString() + " is " + totalNumberOfTuples);
		return totalNumberOfTuples;
	}
	
	private int calculateFullnessThreshold() {
		return 20000;
	}
	
	private int calculateEmptinessThreshold() {
		return 0;
	}
	
	public boolean evaluateEmptiness() throws Exception {
		DBInvoker dbInvoker = new DBInvoker(LogLevel.STATUS, this, null);
		
		System.out.print("Checking for the emptiness of the  DB " + toString() + ": ");
		Set<String> allUserSchemas = dbInvoker.fetchAllUserSchemas();
		Set<Pair<String,String>> allSchemaTables = new HashSet<Pair<String,String>>();
		for (String schema : allUserSchemas) {
			 Set<String> tables = dbInvoker.fetchAllTables(schema);
			 for (String table : tables)
				 allSchemaTables.add(new Pair<String, String>(schema, table));
		}
		boolean isEmpty = true;
		for (Pair<String, String> pair : allSchemaTables) {
			if (!dbInvoker.isThisTableEmpty(pair.first, pair.second)) {
				isEmpty = false;
				System.out.print("Table " + pair.first + "." + pair.second + " is not empty. "); 
				break;
			}
		}
		System.out.println(" this DB is " + (isEmpty? "" : "NOT ") + "empty.");
		return isEmpty;
	}

	public static DatabaseLoginConfiguration getFullDB(List<DatabaseLoginConfiguration> allAvailableConfigs, String relevantDBName) throws Exception {
		DatabaseLoginConfiguration fullDB = null;
		for (DatabaseLoginConfiguration db : allAvailableConfigs)
			if (db.getDBname().equals(relevantDBName))
				if (db.isEmpty())
					continue;
				else if (fullDB == null)
					fullDB = db;
				else
					throw new Exception("You have indicated more than one full DB: " + fullDB + "\nand\n" + db);
		
		if (fullDB==null)
			throw new Exception("Could not find a full DB to return!");
		else if (safeMode && fullDB.evaluateEmptiness())
			throw new Exception("We found " + fullDB + " declared as a full database, but it turned out that it is an empty one.");
		else {
			return fullDB;
		}
	}

	public static List<DatabaseLoginConfiguration> getEmptyDBs(List<DatabaseLoginConfiguration> allAvailableConfigs) throws Exception {
		List<DatabaseLoginConfiguration> emptyDBs = new ArrayList<DatabaseLoginConfiguration>();
		for (DatabaseLoginConfiguration db : allAvailableConfigs) 
			if (db.isEmpty())
				emptyDBs.add(db);
		
			
		if (emptyDBs.isEmpty())
			throw new Exception("Could not find any empty DBs to return!");
		
		if (safeMode)
			for (DatabaseLoginConfiguration db : emptyDBs)
				if (!db.evaluateEmptiness())
					throw new Exception("We found " + db + " declared as an empty database, but it turned out that it contains: " + db.getTotalNumberOfUserTuples());

		return emptyDBs;
	}
	
	public static List<DatabaseLoginConfiguration> getEmptyDBs(List<DatabaseLoginConfiguration> allAvailableConfigs, String relevantDBName) throws Exception {
		List<DatabaseLoginConfiguration> emptyDBs = new ArrayList<DatabaseLoginConfiguration>();
		for (DatabaseLoginConfiguration db : allAvailableConfigs) 
			if (db.getDBname().equals(relevantDBName))
				if (db.isEmpty())
					emptyDBs.add(db);
		
			
		if (emptyDBs.isEmpty())
			throw new Exception("Could not find any empty DBs to return!");
		
		if (safeMode)
			for (DatabaseLoginConfiguration db : emptyDBs)
				if (!db.evaluateEmptiness())
					throw new Exception("We found " + db + " declared as an empty database, but it turned out that it contains: " + db.getTotalNumberOfUserTuples());

		return emptyDBs;
	}
	
	public static DatabaseLoginConfiguration findDBNameInList(String dbName, List<DatabaseLoginConfiguration> listOfDBConfigs) {
		DatabaseLoginConfiguration result = null;
		for (DatabaseLoginConfiguration dbConfig : listOfDBConfigs)
			if (dbConfig.getDBname().equals(dbName))
				result = dbConfig;
		return result;
	}
	
	public static DatabaseLoginConfiguration findDBAliasInList(String dbAlias, List<DatabaseLoginConfiguration> listOfDBConfigs) {
		DatabaseLoginConfiguration result = null;
		for (DatabaseLoginConfiguration dbConfig : listOfDBConfigs)
			if (dbConfig.getDBalias().equals(dbAlias))
				result = dbConfig;
		return result;
	}

	public static String getDatabaseSpecificLoginName(String DBVendor) throws Exception {
		if (DBVendor.equals("vertica"))
			return VerticaDatabaseLoginConfiguration.class.getSimpleName();
		else if (DBVendor.equals("microsoft"))
			return MicrosoftDatabaseLoginConfiguration.class.getSimpleName();
		else
			throw new Exception("DBVendor " + DBVendor + " is not valid");
	}
	
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    // each DB vendor implements the following function accordingly!
	public abstract Connection createConnection() throws Exception;
	
	public abstract Map<String, Schema> getSchemaMap() throws Exception;

	
}
