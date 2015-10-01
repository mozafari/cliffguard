package edu.umich.robustopt.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaLoader;
import com.relationalcloud.tsqlparser.loader.SchemaTable;

import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;


public class SchemaUtils {

	// Currently we do not check whether the dbName in the file is equal to the given dbName
	private static Map<String, Schema> GetSchemaMapFromCacheFile(String dbName, String schemaCacheFile) {
		Map<String, Schema> schemaMap = null;
		File serializedSchemaFile = new File(schemaCacheFile);
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(serializedSchemaFile));
			schemaMap = (Map<String, Schema>) in.readObject();
			in.close();
		} catch (ClassNotFoundException e) {

		} catch (IOException e) {
			
		}
		return schemaMap;
	}

	public static SchemaDescriptor GetSchemaMapFromDefaultSources(String dbName, String VendorSpecificDatabaseLoginName) throws Exception {
		List<DatabaseLoginConfiguration> defaultDatabases = DatabaseLoginConfiguration.loadDatabaseConfigurations(GlobalConfigurations.RO_BASE_PATH + File.separator + "databases.conf", VendorSpecificDatabaseLoginName);
		DatabaseLoginConfiguration dbLogin = (defaultDatabases.isEmpty()? null : defaultDatabases.get(0));
		return GetSchemaMap(dbName, dbLogin);
	}

	public static SchemaDescriptor GetSchemaMap(String dbName, DatabaseLoginConfiguration dbLogin) throws Exception {
		File cacheDirForSchema = new File(GlobalConfigurations.RO_BASE_CACHE_PATH + File.separator + dbName + ".schema.ser");
		if (!cacheDirForSchema.getParentFile().exists()) {
			cacheDirForSchema.getParentFile().mkdirs();
		}
		return GetSchemaMap(dbName, GlobalConfigurations.RO_BASE_CACHE_PATH + "/" + dbName+".schema.ser", dbLogin);
	}

	
	public static SchemaDescriptor GetSchemaMap(String dbName, List<DatabaseLoginConfiguration> dbLogins) throws Exception {
		File cacheDirForSchema = new File(GlobalConfigurations.RO_BASE_CACHE_PATH + File.separator + dbName + ".schema.ser");
		if (!cacheDirForSchema.getParentFile().exists()) {
			cacheDirForSchema.getParentFile().mkdirs();
		}
		DatabaseLoginConfiguration dbLogin = (dbLogins==null || dbLogins.isEmpty()? null : dbLogins.get(0));

		return GetSchemaMap(dbName, GlobalConfigurations.RO_BASE_CACHE_PATH + File.separator + dbName+".schema.ser", dbLogin);
	}

	public static SchemaDescriptor GetSchemaMap(String dbName, String schemaCacheFile, DatabaseLoginConfiguration dbLogin) throws Exception {

		Connection conn = null; // this value will remain null if we do not need to get the schema from the DB!
		Map<String, Schema> schemaMap = null;

		schemaMap = GetSchemaMapFromCacheFile(dbName, schemaCacheFile);

		if (schemaMap == null) {
			System.out.println("Reading schema map from database");
			schemaMap = dbLogin.getSchemaMap();
			// save to file
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(schemaCacheFile));
			oos.writeObject(schemaMap);
			oos.close();				
		} else {
			System.out.println("Read schema map from file");
		}

		return new SchemaDescriptor(conn, schemaMap);
	}
	
	public static Map<String, Schema> GetTPCHSchema() {
		// XXX: hard-coded for now
/*
		CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                L_PARTKEY     INTEGER NOT NULL,
                L_SUPPKEY     INTEGER NOT NULL,
                L_LINENUMBER  INTEGER NOT NULL,
                L_QUANTITY    DECIMAL(15,2) NOT NULL,
                L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                L_TAX         DECIMAL(15,2) NOT NULL,
                L_RETURNFLAG  CHAR(1) NOT NULL,
                L_LINESTATUS  CHAR(1) NOT NULL,
                L_SHIPDATE    DATE NOT NULL,
                L_COMMITDATE  DATE NOT NULL,
                L_RECEIPTDATE DATE NOT NULL,
                L_SHIPINSTRUCT CHAR(25) NOT NULL,
                L_SHIPMODE     CHAR(10) NOT NULL,
                L_COMMENT      VARCHAR(44) NOT NULL,
                PRIMARY KEY (L_ORDERKEY, L_LINENUMBER));
        -- force a super-projection to be created, by inserting
		insert into lineitem values (0, 0, 0, 0, 0, 0, 0, 0, 'a', 'a', sysdate(), sysdate(), sysdate(), 'a', 'a', 'a');
		-- delete the dummy value we inserted
		truncate table LINEITEM;
*/
		
		SchemaTable t = new SchemaTable("public", "lineitem");
		t.addColumn("l_orderkey", "INTEGER");
		t.addColumn("l_partkey", "INTEGER");
		t.addColumn("l_suppkey", "INTEGER");
		t.addColumn("l_linenumber", "INTEGER");
		t.addColumn("l_quantity", "DECIMAL(15,2)");
		t.addColumn("l_extendedprice", "DECIMAL(15,2)");
		t.addColumn("l_discount", "DECIMAL(15,2)");
		t.addColumn("l_tax", "DECIMAL(15,2)");
		t.addColumn("l_returnflag", "CHAR(1)");
		t.addColumn("l_linestatus", "CHAR(1)");
		t.addColumn("l_shipdate", "DATE");
		t.addColumn("l_commitdate", "DATE");
		t.addColumn("l_receiptdate", "DATE");
		t.addColumn("l_shipinstruct", "CHAR(25)");
		t.addColumn("l_shipmode", "CHAR(10)");
		t.addColumn("l_comment", "VARCHAR(44)");
		
		Schema s = new Schema();		
		s.addTable(t);
		
		Map<String, Schema> ret = new HashMap<String, Schema>();
		ret.put("public", s);
		return ret;
	}
	


}
