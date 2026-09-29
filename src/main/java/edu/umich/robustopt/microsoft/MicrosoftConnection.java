package edu.umich.robustopt.microsoft;

import java.sql.Connection;
import java.sql.DriverManager;

import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;

public class MicrosoftConnection {

	public static Connection createConnection(DatabaseLoginConfiguration databaseLogin) {
		Connection conn = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			conn = DriverManager.getConnection(
					String.format("jdbc:sqlserver://%s:%s;databasename=%s", 
							databaseLogin.getDBhost(), databaseLogin.getDBport(), databaseLogin.getDBname()), 
							databaseLogin.getDBuser(), databaseLogin.getDBpasswd());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return conn;
	}
}
