package edu.umich.robustopt.vertica;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.util.NamedIdentifier;

/**
 * Represents an actual, deployed vertica projection, identified
 * by name. Contains structural information
 * 
 * @author stephentu
 *
 */
public class VerticaDeployedProjection extends DeployedPhysicalStructure {
		
	public VerticaDeployedProjection(
			String projection_schema,
			String projection_name,
			String projection_basename,
			VerticaProjectionStructure projection_structure) {
		super(projection_schema, projection_name, projection_basename, projection_structure);
	}
	
	/**
	 * projection_schema.projection_basename (logical name)
	 * @return
	 */
	public NamedIdentifier getProjectionIdent() {
		return new NamedIdentifier(getSchema(), getBasename());
	}
	
	
	
	
	@Override
	public String toString() {
		String	structStr = getStructure().toString();
		
		return "VerticaProjection [projection_schema=" + getSchema()
				+ ", projection_name=" + getName()
				+ ", projection_basename=" + getBasename()
				+ ", projection_structure=" + structStr + "]";
	}

	public static VerticaDeployedProjection BuildFrom(Connection conn, String projection_schema, String projection_basename_or_name, boolean isBasename) throws SQLException {
//		dbadmin=> select pc.projection_column_name, column_position, sort_position, data_type from v_catalog.projections p join v_catalog.projection_columns pc on (p.projection_name = pc.projection_name) where p.projection_schema = 'public' and p.projection_basename = 'LINEITEM_DBD_2_rep_example_1_designname_1' order by pc.column_position;
//		 projection_column_name | column_position | sort_position |   data_type   
//		------------------------+-----------------+---------------+---------------
//		 L_ORDERKEY             |               0 |               | int
//		 L_PARTKEY              |               1 |               | int
//		 L_SUPPKEY              |               2 |               | int
//		 L_LINENUMBER           |               3 |               | int
//		 L_QUANTITY             |               4 |               | numeric(15,2)
//		 L_EXTENDEDPRICE        |               5 |               | numeric(15,2)
//		 L_DISCOUNT             |               6 |               | numeric(15,2)
//		 L_TAX                  |               7 |               | numeric(15,2)
//		 L_RETURNFLAG           |               8 |               | char(1)
//		 L_LINESTATUS           |               9 |               | char(1)
//		 L_SHIPDATE             |              10 |               | date
//		 L_COMMITDATE           |              11 |               | date
//		 L_RECEIPTDATE          |              12 |             1 | date
//		 L_SHIPINSTRUCT         |              13 |               | char(25)
//		 L_SHIPMODE             |              14 |             0 | char(10)
//		 L_COMMENT              |              15 |               | varchar(44)
//		(16 rows)

		String anchor_table_schema = null, anchor_table_name = null, projection_name = null, projection_basename = null;
		
		List<String> columns = new ArrayList<String>();
		List<String> ctypes  = new ArrayList<String>();
		List<String> cencs   = new ArrayList<String>();
		Map<Integer, Integer> csortmap = new HashMap<Integer, Integer>();
		
		Statement stmt = null;
		ResultSet res = null;
		String sql = null;
		try {
			stmt = conn.createStatement();
/*
			select pc.projection_column_name, pc.column_position, pc.sort_position, pc.data_type, pc.encoding_type 
			from v_catalog.projections p join v_catalog.projection_columns pc on (p.projection_name = pc.projection_name) 
				join v_catalog.tables t on (p.anchor_table_id = t.table_id and pc.table_schema = t.table_schema and pc.table_name = t.table_name) 
			where p.projection_schema = 'atheneasset' and p.projection_basename= 'ident_89_DBD_260_rep_example_designname' order by pc.column_position;
			
*/	
			sql = 
				"select t.table_schema, p.anchor_table_name, p.projection_name, p.projection_basename " +
				"from v_catalog.projections p join v_catalog.tables t on (p.anchor_table_id = t.table_id) " +
				"where p.projection_schema = '" + projection_schema + "' and p.projection"+ (isBasename? "_basename":"_name") + "= '" + projection_basename_or_name + "';";
			res = stmt.executeQuery(sql);
			int rc = 0;
			while (res.next()) {
				anchor_table_schema = res.getString(1);
				anchor_table_name   = res.getString(2);
				projection_name     = res.getString(3);
				projection_basename = res.getString(4);
				++rc;
			}
			if (rc!=1)
				throw new SQLException("the following command returned " + rc +" rows while we were expecting 1 row: " + sql);
			
			sql = 
				"select pc.projection_column_name, pc.column_position, pc.sort_position, pc.data_type, pc.encoding_type " +
				"from v_catalog.projections p join v_catalog.projection_columns pc on (p.projection_name = pc.projection_name) " +
				"where p.projection_schema = '" + projection_schema + "' and p.projection"+ (isBasename? "_basename":"_name") + "= '" + projection_basename_or_name + "' " +
				"and pc.table_schema = '" + anchor_table_schema +"' and pc.table_name = '" + anchor_table_name + "' " +
				"order by pc.column_position";
			
			
			
			
			res = stmt.executeQuery(sql);
			int rc2 = 0;
			while (res.next()) {
				String column = res.getString(1);
				if (columns.contains(column))
					throw new SQLException("Error: Projection " + projection_schema + "." + projection_basename_or_name + " contained the following column more than once: " + column + "\nThe sql query was: " + sql);
				columns.add(column);
				ctypes.add(res.getString(4));
				cencs.add(res.getString(5));
				if (res.getString(3) != null) 
					csortmap.put(res.getInt(3), res.getInt(2));
				++rc2;
			}
			if (rc2<1)
				throw new SQLException("This query returned no results: " + sql);
		} finally {
			if (stmt != null)
				stmt.close();
			if (res != null)
				res.close();
		}
		
		if (columns.isEmpty())
			throw new SQLException("There were no columns when running: " + sql);
		
		String[] csortarr = new String[csortmap.size()];
		for (Map.Entry<Integer, Integer> e : csortmap.entrySet())
			csortarr[e.getKey()] = columns.get(e.getValue());
		List<String> csort = Arrays.asList(csortarr);
		
		return new VerticaDeployedProjection(
				projection_schema, projection_name, projection_basename, 
				new VerticaProjectionStructure(
						new NamedIdentifier(anchor_table_schema, anchor_table_name), 
						columns, ctypes, cencs, csort));
	}

	@Override
	public DeployedPhysicalStructure clone() {	
		return new VerticaDeployedProjection(getSchema(), getName(), getBasename(), (VerticaProjectionStructure) getStructure());
	}
	
	
}
