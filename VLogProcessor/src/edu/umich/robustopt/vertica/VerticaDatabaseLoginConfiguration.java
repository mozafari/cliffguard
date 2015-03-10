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
		return VerticaConnection.createConnection(this);
	}

	@Override
	public Map<String, Schema> getSchemaMap() throws SQLException {
		Connection conn = null;
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
