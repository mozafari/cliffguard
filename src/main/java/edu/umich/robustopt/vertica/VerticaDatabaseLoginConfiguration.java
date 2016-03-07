package edu.umich.robustopt.vertica;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaLoader;

import edu.umich.robustopt.dblogin.*;

public class VerticaDatabaseLoginConfiguration extends DatabaseLoginConfiguration {
	protected static final int TIME_OUT_IN_SECONDS = 1;
	public VerticaDatabaseLoginConfiguration(boolean empty, String DBalias,
			String DBhost, Integer DBport, String DBname, String DBuser,
			String DBpasswd) {
		super(empty, DBalias, DBhost, DBport, DBname, DBuser, DBpasswd);
		// TODO Auto-generated constructor stub
	}

	public VerticaDatabaseLoginConfiguration() {
		// empty one!
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public Connection createConnection() throws Exception {
		Connection dbConnection = null;
		try {
			Class.forName("com.vertica.jdbc.Driver");
			dbConnection = new ResiliantConnection("jdbc:vertica://" + getDBhost() + ":" + getDBport() + 
					"/"+getDBname()+"?user=" + getDBuser() +"&password="+getDBpasswd(), TIME_OUT_IN_SECONDS);
		} catch (Exception e) {
			printConnectionError(getDBhost());
			throw e;
		}
		return dbConnection;
	}
	
	private void printConnectionError(String DBhost) {
		System.err.println("Could not establish connection to server: " + DBhost + 
				"\nMake sure the server is running, and you are connected to internet.\n");
		
		/*System.err.println("Also try to run:");
		for (String hostName : hostNames.keySet())
			System.err.println("ssh -f -N -c blowfish -C -L "+ portNumbers.get(hostName) +":localhost:5433 barzan@"+hostNames.get(hostName));
		*/
		System.err.println("Alternatively, make sure your DB's authentication configuration is:\n");
		System.err.println("ClientAuthentication = local all trust\nClientAuthentication = host all 0.0.0.0/0 md5");
		
	}

	@Override
	public Map<String, Schema> getSchemaMap() throws Exception {
		Connection conn = createConnection();
		// list schemas
		List<String> schemas = new ArrayList<String>();
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery("select schema_name from v_catalog.schemata where is_system_schema = 'f'");
		while (res.next())
			schemas.add(res.getString(1));
		res.close();
		stmt.close();

		HashMap<String, Schema> schemaMap = new HashMap<String, Schema>();
		for (String s : schemas) {
			Schema sch = SchemaLoader.loadSchemaFromDB(conn, s);
			schemaMap.put(s, sch);
		}
		
		return schemaMap;
	}

}
