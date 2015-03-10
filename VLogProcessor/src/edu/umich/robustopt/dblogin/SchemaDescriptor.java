package edu.umich.robustopt.dblogin;

import java.sql.Connection;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;

public class SchemaDescriptor {

	private final Connection connection;
	private final Map<String, Schema> schemas;
	
	public SchemaDescriptor(Connection connection, Map<String, Schema> schemas) {
		this.connection = connection;
		this.schemas = schemas;
		if (connection == null || schemas == null) 
			System.err.println("Connection or schemas are null in SchemaDescriptor");
	}

	public Connection getConnection() {
		return connection;
	}

	public Map<String, Schema> getSchemas() {
		return schemas;
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		
		buf.append(getSummary() + "List of Tables:\n");

		for (Schema schema : schemas.values()) {
			for (String tabName : schema.getAllTables()) {
				buf.append(schema.getSchemaName() + "." + tabName + "\n");
			}
		}
		
		return buf.toString();
	}
	
	public String getSummary() {
		int totalTables = 0;
		String schemaNames = "";
		for (Schema schema : schemas.values()) {
			schemaNames += schema.getSchemaName() + ",";
			totalTables += schema.getAllTables().size();
		}
		return "SchemaDescriptor: " + schemas.size() + " schemas and " + totalTables + " tables\nschemas (" + schemaNames + ")\n";
	}
	
}
