package edu.umich.robustopt.workloads;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.SchemaDescriptor;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.workloads.QueryConstantRewriter.RewriteException;


import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;


public class ConstantValueManager {
	public static final String RO_BASE_CACHE_PATH = GlobalConfigurations.RO_BASE_CACHE_PATH;
	
	private SchemaDescriptor schemaMap;
	private final double sample; 
	private Map<String, ValueDistribution> cachedDistibutions = new HashMap<String, ValueDistribution>();
	
	public ConstantValueManager(SchemaDescriptor schemaMap, Map<String, ValueDistribution> cachedDistibutions, double sample) throws Exception {
		this.schemaMap = schemaMap;
		this.cachedDistibutions = cachedDistibutions;
		if (sample < 0.0 || sample > 1.0)
			throw new IllegalArgumentException("bad sample");
		this.sample = sample;
	}
	
	public ConstantValueManager(String dbName, double sample, String databaseLoginFile, String VendorDatabaseLoginName) throws Exception {
		this(
				(databaseLoginFile==null? 
						SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VendorDatabaseLoginName) 
						: SchemaUtils.GetSchemaMap(dbName, DatabaseLoginConfiguration.loadDatabaseConfigurations(databaseLoginFile, VendorDatabaseLoginName))
				), 
				loadDataDistribution(dbName, 
									 (databaseLoginFile==null? SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VendorDatabaseLoginName) 
											 				: SchemaUtils.GetSchemaMap(dbName, DatabaseLoginConfiguration.loadDatabaseConfigurations(databaseLoginFile, VendorDatabaseLoginName))), 
									 sample, 
									 (databaseLoginFile==null? null : DatabaseLoginConfiguration.loadDatabaseConfigurations(databaseLoginFile, VendorDatabaseLoginName))), 
				sample);
	}
	
	public ConstantValueManager(String dbName, Map<String, ValueDistribution> cachedDists, double samplingRateAppliedToCreateDistribution, String databaseLoginFile, String VendorDatabaseLoginName) throws Exception {
		this((databaseLoginFile==null? 
				SchemaUtils.GetSchemaMapFromDefaultSources(dbName, VendorDatabaseLoginName) 
				: SchemaUtils.GetSchemaMap(dbName, DatabaseLoginConfiguration.loadDatabaseConfigurations(databaseLoginFile, VendorDatabaseLoginName))), 
				cachedDists, samplingRateAppliedToCreateDistribution);
	}

	private static Map<String, ValueDistribution> loadDataDistribution(String dbName, SchemaDescriptor schemaMap, double sample, List<DatabaseLoginConfiguration> databaseLogins) throws Exception {	
		Map<String, ValueDistribution> distributionMap = null;

		long samplePercentage = Math.round(sample*100);
		File serializedDistributionFile = new File(RO_BASE_CACHE_PATH, dbName+".data_distribution.sampled-"+samplePercentage+".ser");
		
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(serializedDistributionFile));
			distributionMap = (Map<String, ValueDistribution>) in.readObject();
			in.close();
		} catch (ClassNotFoundException e) {

		} catch (IOException e) {
			
		}
		if (distributionMap == null) {
			System.out.println("WARNING: Need to read the data distribution from the database");
			
			//find the list of all columns in the schema
			Set<ColumnDescriptor> allColumns = new HashSet<ColumnDescriptor>();			
			for (String schemaName : schemaMap.getSchemas().keySet()) {
				Schema schema = schemaMap.getSchemas().get(schemaName);
				for (SchemaTable table : schema.getTables())
					for (String columnName : table.getColumns()) {
						ColumnDescriptor column = new ColumnDescriptor(schemaName, table.getTableName(), columnName);
						assert (!allColumns.contains(column)); // otherwise we have not created these columns correctly!
						allColumns.add(column);
					}
			}

			// create a connection to the database
			if (databaseLogins==null || databaseLogins.isEmpty())
				throw new Exception("We did not find the ConstantValueManager in a file, and you did not provide a valid database login! " + databaseLogins);
			DatabaseLoginConfiguration fullDB = DatabaseLoginConfiguration.getFullDB(databaseLogins, dbName);
			Connection conn = fullDB.createConnection();
			distributionMap = new HashMap<String, ValueDistribution>();
			// now retrive the distribution of all columns from the database
			for (ColumnDescriptor column : allColumns) {
				ValueDistribution cd = getColumnDistributionFromDB(conn, column, sample);
				distributionMap.put(column.getQualifiedName(), cd);
			}
			
			// save to file
			saveDataDistributionsToFile(distributionMap, serializedDistributionFile);
			
		} else {
			System.out.println("We loaded the data distribution from file");
		}

		return distributionMap;
	}

	public static void saveDataDistributionsToFile(Map<String, ValueDistribution> distributionMap, File f) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
		oos.writeObject(distributionMap);
		oos.close();
	}

	public void saveDataDistributionsToFile(File f) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
		oos.writeObject(cachedDistibutions);
		oos.close();
	}

	public static ConstantValueManager RestoreStateFromFile(String dbName, double samplingRateAppliedToCreateDistribution, File f, String dbLoginConfigFile, String VendorSpecificDatabaseLoginName) 
			throws Exception {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
		Map<String, ValueDistribution> cachedDists = (Map<String, ValueDistribution>) ois.readObject();
		ois.close();
		ConstantValueManager ret = new ConstantValueManager(dbName, cachedDists, samplingRateAppliedToCreateDistribution, dbLoginConfigFile, VendorSpecificDatabaseLoginName);
		ret.cachedDistibutions.putAll(cachedDists);
		return ret;
	}
	
	public ValueDistribution getColumnDistribution(ColumnDescriptor cd) throws Exception {
		
		ValueDistribution vd = cachedDistibutions.get(cd.getQualifiedName());
		if (vd != null) {
			//System.out.println("Cache hit: " + cd);
			//System.out.println("Cache size: " + cachedDists.size());
			return vd;
		} else 
			throw new Exception("We do not have the distribution of the following column: " + cd);
	}

	private static ValueDistribution getColumnDistributionFromDB(Connection conn, ColumnDescriptor cd, double sample) {
		// find distribution of value
		List<Object> values = new ArrayList<Object>();
		List<Integer> counts = new ArrayList<Integer>();
		java.sql.Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String samplePiece = sample < 1.0 ? "AND random() < " + sample + " " : "";
			String sql = "SELECT " + cd.getColumnName() + ", COUNT(*) " + 
					"FROM " + cd.getQualifiedTableName() + " " + 
					"WHERE " + cd.getColumnName() + " IS NOT NULL " + samplePiece +
					"GROUP BY " + cd.getColumnName();
			Timer t = new Timer();
			System.out.println("Executing sql: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("... finished in " + t.lapMillis() + " ms");
			ResultSetMetaData md = rs.getMetaData();
			while (rs.next()) {
				switch (md.getColumnType(1)) {
				case Types.BOOLEAN:
					values.add(rs.getBoolean(1));
					break;
					
				case Types.BINARY:
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.VARBINARY:
					values.add(rs.getString(1));
					break;

				case Types.DECIMAL:
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.NUMERIC:
					values.add(rs.getDouble(1));
					break;
					
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
					values.add(rs.getInt(1));
					break;
				
				case Types.BIGINT:
					values.add(rs.getLong(1));
					break;
					
				case Types.TIME:
					values.add(rs.getTime(1));
					break;
				
				case Types.TIMESTAMP:
					try {
						values.add(rs.getTimestamp(1));
					} catch (SQLException e2) {
						System.err.println("Could not handle: " + sql + "\nof type Types.TIMESTAMP with  value: " + rs.getString(1));
						continue;
					}
					
					break;
				
				case Types.DATE:
					try {
						values.add(rs.getDate(1));
					} catch (SQLException e2) {
						System.err.println("Could not handle: " + sql + "\nof type Types.DATE with  value: " + rs.getString(1));
						continue;
					}
					break;
					
				default:
					throw new RewriteException("unhandled type: " + md.getColumnTypeName(1));
				}
				counts.add(rs.getInt(2));
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			throw new RewriteException(e);
		}
		
		//System.out.println(values);
		//System.out.println(counts);
		
		ValueDistribution vd = values.isEmpty() ? null : new ValueDistribution(values, counts);
		return vd;
	}
}
