package edu.umich.robustopt.clustering;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaLoader;

import edu.umich.robustopt.clustering.Query_v2;
import edu.umich.robustopt.staticanalysis.ColumnExtractor;
import edu.umich.robustopt.util.VerticaConnection;



public class TestQueryExtractor {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		Map<String, Schema> schemaMap = null;
		File serializedSchemaFile = new File("data", "testdb1.schema.ser");
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(serializedSchemaFile));
			schemaMap = (Map<String, Schema>) in.readObject();
			in.close();
		} catch (ClassNotFoundException e) {
			
		} catch (IOException e) {
			
		}
		
		if (schemaMap == null) {
			System.out.println("Need to read schema map from database");
			Connection conn = VerticaConnection.createDefaultConnection("vm_empty_db");

			// list schemas
			List<String> schemas = new ArrayList<String>();
			Statement stmt = conn.createStatement();
			ResultSet res = stmt.executeQuery("select schema_name from v_catalog.schemata where is_system_schema = 'f'");
			while (res.next())
				schemas.add(res.getString(1));
			res.close();
			stmt.close();

			schemaMap = new HashMap<String, Schema>();
			for (String s : schemas) {
				Schema sch = SchemaLoader.loadSchemaFromDB(conn, s);
				schemaMap.put(s, sch);
			}

			// save to file
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serializedSchemaFile));
			oos.writeObject(schemaMap);
			oos.close();
			
		} else {
			System.out.println("Read schema map from file");
		}
		
		//Parser p = new Parser("public", null, "select  sum(case when a11.example_column='k56' then 1 else 0 end) AS example_column,   sum(a11.example_column) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then age_in_months(a11.example_column,a11.example_column) * a11.example_column end) AS WJXBFS1,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column) else null end) AS WJXBFS2,  sum(a11.example_column) AS example_column,   sum(case when a11.example_column='e/M' then a11.example_column else 0 end) AS example_column,  sum(Case when age_in_months(a11.example_column,a11.example_column) > 0 AND (a11.example_column >0) and a11.example_column ='V9Y' then  a11.example_column end) AS WJXBFS3,   sum(case when a11.example_column in ('EsE','HqM','Uew','e/M') then a11.example_column else null end) AS example_column,   sum(a11.example_column) AS CURRINVBAL1MAGO,  sum(Case when a11.example_column in ('EsE','HqM','Uew','e/M') and a11.example_column=a11.example_column then a11.example_column end) AS PAYOFFBALANCECDR,   sum(Case when a11.example_column = a11.example_column and a11.example_column = 'e/M' then (a11.example_column * a11.example_column) else  null end) AS WJXBFS4,   sum(case when a11.example_column is not null and a11.example_column in (6,14) then Case when a11.example_column=a11.example_column then a11.example_column end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.example_column='Uew' then a11.example_column else 0 end) AS example_column from example_schema.example_column  a11 where a11.example_column in (278) LIMIT 1000001");
		//Parser p = new Parser("public", null, "SELECT example_column.example_column,        example_column.example_column,        example_column.example_column,        example_column.example_column   FROM example_schema.example_column example_column        JOIN (SELECT example_column,                     MAX(example_column) example_column                FROM example_schema.example_column               GROUP BY example_column) mx          ON example_column.example_column = mx.example_column         AND example_column.example_column = mx.example_column  ORDER BY (CASE                 WHEN example_column.example_column = 'gzI4bd9739' THEN 1                 WHEN example_column.example_column = 'Wx+' THEN 2                 WHEN example_column.example_column = 'hSs87a1cc862' THEN 3                 ELSE 4            END) ASC");
		//Parser p = new Parser("public", null, "SELECT  max(example_column.example_column) AS example_column,  example_column.example_column AS example_column FROM example_schema.example_column WHERE example_column.example_column > 0 GROUP BY example_column.example_column ORDER BY example_column,example_column");
		
		Parser p = new Parser("public", null, "SELECT distinct  csm.example_column,  LTRIM(RTRIM(csm.example_column)),  csm.example_column,  ir.example_column FROM  (select cs.example_column, cs.example_column, cs.example_column, cs.example_column, cs.example_column from example_schema.example_column cs    join (select example_column, max(example_column) mxperiod from example_schema.example_column group by example_column) csp       on cs.example_column = csp.example_column AND cs.example_column = csp.mxperiod) csm  LEFT JOIN example_schema.example_column ir ON ir.example_column = csm.example_column ORDER BY  csm.example_column, csm.example_column");
		com.relationalcloud.tsqlparser.statement.Statement stmt = p.stmt;
		
		ColumnExtractor ex = new ColumnExtractor(schemaMap);
		Query_v2 summary = ex.getColumnSummary(stmt);		
		System.out.println(summary);
		
		p = new Parser("public", null, "SELECT  LTRIM(RTRIM(csm.example_column)), csm.example_column,    csm.example_column,  ir.example_column FROM  (select cs.example_column, cs.example_column, cs.example_column, cs.example_column, cs.example_column from example_schema.example_column cs    join (select example_column, max(example_column) mxperiod from example_schema.example_column group by example_column) csp       on cs.example_column = csp.example_column AND cs.example_column = csp.mxperiod) csm  LEFT JOIN example_schema.example_column ir ON ir.example_column = csm.example_column ORDER BY  csm.example_column");
		stmt = p.stmt;
		ex = new ColumnExtractor(schemaMap);
		Query_v2 q2 = ex.getColumnSummary(stmt);
		System.out.println(summary.equals(q2) + " " + q2.equals(summary));
		
		
		
	}

}
