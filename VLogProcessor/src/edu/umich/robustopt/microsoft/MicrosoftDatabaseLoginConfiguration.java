package edu.umich.robustopt.microsoft;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaLoader;

import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaConnection;

public class MicrosoftDatabaseLoginConfiguration extends
		DatabaseLoginConfiguration {

	String SQLServerName;

	public MicrosoftDatabaseLoginConfiguration() {
		SQLServerName = "";
	}

	public MicrosoftDatabaseLoginConfiguration(boolean empty, String DBalias,
			String DBhost, Integer DBport, String DBname, String DBuser,
			String DBpasswd, String SQLServerName) {
		super(empty, DBalias, DBhost, DBport, DBname, DBuser, DBpasswd);

		this.SQLServerName = SQLServerName;
	}

	public String getSQLServerName() {
		return SQLServerName;
	}


	@Override
	public Connection createConnection() {
		Connection conn = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			conn = DriverManager.getConnection(
					String.format("jdbc:sqlserver://%s:%s;databasename=%s", 
							getDBhost(), getDBport(), getDBname()), getDBuser(), getDBpasswd());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return conn;
	}

	@Override
	public Map<String, Schema> getSchemaMap() throws SQLException {
		Connection conn = createConnection();

		// list schemas
		List<String> schemas = new ArrayList<String>();
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery("select name from sys.schemas");
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
