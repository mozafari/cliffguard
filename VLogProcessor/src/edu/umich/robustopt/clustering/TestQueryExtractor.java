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

	private static final String RO_BASE_PATH = "/Users/sina/andrew/robust-opt"; //"/Users/stephentu/robust-opt";

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
		
		//Parser p = new Parser("public", null, "select  sum(case when a11.ident_2629='k56' then 1 else 0 end) AS ident_199,   sum(a11.ident_2090) AS ident_2272,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then age_in_months(a11.ident_421,a11.ident_225) * a11.ident_305 end) AS WJXBFS1,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_305) else null end) AS WJXBFS2,  sum(a11.ident_2386) AS ident_445,   sum(case when a11.ident_1980='e/M' then a11.ident_2090 else 0 end) AS ident_386,  sum(Case when age_in_months(a11.ident_421,a11.ident_225) > 0 AND (a11.ident_305 >0) and a11.ident_425 ='V9Y' then  a11.ident_305 end) AS WJXBFS3,   sum(case when a11.ident_1980 in ('EsE','HqM','Uew','e/M') then a11.ident_2090 else null end) AS ident_1869,   sum(a11.ident_552) AS CURRINVBAL1MAGO,  sum(Case when a11.ident_1612 in ('EsE','HqM','Uew','e/M') and a11.ident_2251=a11.ident_1817 then a11.ident_2471 end) AS PAYOFFBALANCECDR,   sum(Case when a11.ident_1072 = a11.ident_2251 and a11.ident_1612 = 'e/M' then (a11.ident_341 * a11.ident_305) else  null end) AS WJXBFS4,   sum(case when a11.ident_2386 is not null and a11.ident_1779 in (6,14) then Case when a11.ident_2251=a11.ident_1817 then a11.ident_552 end end) AS VOLUNTARYPAYOFFBAL,   sum(case when a11.ident_1980='Uew' then a11.ident_2090 else 0 end) AS ident_1637 from st_etl_2.ident_164  a11 where a11.ident_2251 in (278) LIMIT 1000001");
		//Parser p = new Parser("public", null, "SELECT ident_134.ident_1701,        ident_134.ident_298,        ident_134.ident_538,        ident_134.ident_876   FROM st_etl_2.ident_75 ident_134        JOIN (SELECT ident_1701,                     MAX(ident_876) ident_876                FROM st_etl_2.ident_75               GROUP BY ident_1701) mx          ON ident_134.ident_1701 = mx.ident_1701         AND ident_134.ident_876 = mx.ident_876  ORDER BY (CASE                 WHEN ident_134.ident_1701 = 'gzI4bd9739' THEN 1                 WHEN ident_134.ident_1701 = 'Wx+' THEN 2                 WHEN ident_134.ident_1701 = 'hSs87a1cc862' THEN 3                 ELSE 4            END) ASC");
		//Parser p = new Parser("public", null, "SELECT  max(ident_91.ident_1035) AS ident_1035,  ident_91.ident_1187 AS ident_1187 FROM st_etl_2.ident_91 WHERE ident_91.ident_1035 > 0 GROUP BY ident_91.ident_1187 ORDER BY ident_1187,ident_1035");
		
		Parser p = new Parser("public", null, "SELECT distinct  csm.ident_1526,  LTRIM(RTRIM(csm.ident_1773)),  csm.ident_418,  ir.ident_2089 FROM  (select cs.ident_412, cs.ident_1773, cs.ident_1526, cs.ident_1436, cs.ident_418 from st_etl_2.ident_86 cs    join (select ident_412, max(ident_2251) mxperiod from st_etl_2.ident_86 group by ident_412) csp       on cs.ident_412 = csp.ident_412 AND cs.ident_2251 = csp.mxperiod) csm  LEFT JOIN st_etl_2.ident_65 ir ON ir.ident_1587 = csm.ident_1436 ORDER BY  csm.ident_1526, csm.ident_1436");
		com.relationalcloud.tsqlparser.statement.Statement stmt = p.stmt;
		
		ColumnExtractor ex = new ColumnExtractor(schemaMap);
		Query_v2 summary = ex.getColumnSummary(stmt);		
		System.out.println(summary);
		
		p = new Parser("public", null, "SELECT  LTRIM(RTRIM(csm.idenT_1773)), csm.ident_1526,    csm.ident_418,  ir.ident_2089 FROM  (select cs.ident_412, cs.ident_1773, cs.ident_1526, cs.ident_1436, cs.ident_418 from st_etl_2.ident_86 cs    join (select ident_412, max(ident_2251) mxperiod from st_etl_2.ident_86 group by ident_412) csp       on cs.ident_412 = csp.ident_412 AND cs.ident_2251 = csp.mxperiod) csm  LEFT JOIN st_etl_2.ident_65 ir ON ir.ident_1587 = csm.ident_1436 ORDER BY  csm.ident_1526");
		stmt = p.stmt;
		ex = new ColumnExtractor(schemaMap);
		Query_v2 q2 = ex.getColumnSummary(stmt);
		System.out.println(summary.equals(q2) + " " + q2.equals(summary));
		
		
		
	}

}
