package edu.umich.robustopt.vertica;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.ResiliantConnection;
import edu.umich.robustopt.util.Timer;



public class VerticaConnection {
	private static final int TIME_OUT_IN_SECONDS = 1;

	private static HashMap<String, DatabaseLoginConfiguration> fillDefaultLogins() {		
		HashMap<String, DatabaseLoginConfiguration> aliasToDefaultLogins = new HashMap<String, DatabaseLoginConfiguration>();

		DatabaseLoginConfiguration real_full_db = new VerticaDatabaseLoginConfiguration(false, "real_full_db", "vise3.csail.mit.edu", 5433, null, "dbadmin", "letmein");
		aliasToDefaultLogins.put("real_full_db", real_full_db);
		
		DatabaseLoginConfiguration vm_empty_db = new VerticaDatabaseLoginConfiguration(true, "vm_empty_db", "vise4-vm.csail.mit.edu", 5433, null, "dbadmin", "letmein");
		aliasToDefaultLogins.put("vm_empty_db", vm_empty_db);
		
		return aliasToDefaultLogins;
	}

	public static Connection createConnection(DatabaseLoginConfiguration dbConfig) throws Exception {		
		Connection dbConnection = createConnection(dbConfig.getDBhost(), dbConfig.getDBport(), dbConfig.getDBname(), dbConfig.getDBuser(), dbConfig.getDBpasswd());
		return dbConnection;
	}

	public static Connection createConnection(String DBhost, Integer DBport, String DBname, String DBuser, String DBpasswd) throws Exception {		
		Connection dbConnection = null;
		try {
			Class.forName("com.vertica.jdbc.Driver");
			dbConnection = new ResiliantConnection("jdbc:vertica://" + DBhost + ":" + DBport + "/"+DBname+"?user=" + DBuser +"&password="+DBpasswd, TIME_OUT_IN_SECONDS);
		} catch (Exception e) {
			printConnectionError(DBhost);
			throw e;
		}
		return dbConnection;
	}

	public static Connection createRandomConnectionByDBname(String DBname, Collection<DatabaseLoginConfiguration> databaseLogins) throws Exception {
		for (DatabaseLoginConfiguration dbLogin : databaseLogins) {
			DatabaseLoginConfiguration db = (DatabaseLoginConfiguration) dbLogin.clone();
			db.setDBname(DBname);
			try {
				Connection dbConnection = createConnection(db);
				System.out.println("Found " + db + " hosting " + DBname);
				return dbConnection;
			} catch (Exception e) {
				//continue;
			}
		}
		
		throw new Exception("Did not find any servers hosting " + DBname);
	}

	public static Connection createRandomFullConnectionByDBname(String DBname, Collection<DatabaseLoginConfiguration> databaseLogins) throws Exception {
		Collection<DatabaseLoginConfiguration> eligible = new ArrayList<DatabaseLoginConfiguration>();
		for (DatabaseLoginConfiguration dbLogin : databaseLogins) {
			if (dbLogin.isEmpty())
				continue;
			else
				eligible.add(dbLogin);
		}
		return createRandomConnectionByDBname(DBname, eligible);
	}

	public static Connection createRandomEmptyConnectionByDBname(String DBname, Collection<DatabaseLoginConfiguration> databaseLogins) throws Exception {
		Collection<DatabaseLoginConfiguration> eligible = new ArrayList<DatabaseLoginConfiguration>();
		for (DatabaseLoginConfiguration dbLogin : databaseLogins) {
			if (dbLogin.isEmpty())
				eligible.add(dbLogin);
			else
				continue;
		}
		return createRandomConnectionByDBname(DBname, eligible);
	}

	
	public static Connection createDefaultConnectionByDBname(String DBname) throws Exception {
		HashMap<String, DatabaseLoginConfiguration> aliasToDefaultLogins = fillDefaultLogins();
		return createRandomConnectionByDBname(DBname, aliasToDefaultLogins.values());
	}

	public static DatabaseLoginConfiguration createDefaultDBLoginByNameAndServerAlias(String DBname, String server_alias) throws Exception {
		HashMap<String, DatabaseLoginConfiguration> aliasToDefaultLogins = fillDefaultLogins();

		DatabaseLoginConfiguration db = aliasToDefaultLogins.get(server_alias);
		if (db == null) 
			throw new Exception("Could not find any default servers aliased as " + server_alias);
		
		db.setDBname(DBname);
		
		return db;
	}
	
	private static void printConnectionError(String DBhost) {
		System.err.println("Could not establish connection to server: " + DBhost + 
				"\nMake sure the server is running, and you are connected to internet.\n");
		
		/*System.err.println("Also try to run:");
		for (String hostName : hostNames.keySet())
			System.err.println("ssh -f -N -c blowfish -C -L "+ portNumbers.get(hostName) +":localhost:5433 barzan@"+hostNames.get(hostName));
		*/
		System.err.println("Alternatively, make sure your DB's authentication configuration is:\n");
		System.err.println("ClientAuthentication = local all trust\nClientAuthentication = host all 0.0.0.0/0 md5");
		
	}
}
