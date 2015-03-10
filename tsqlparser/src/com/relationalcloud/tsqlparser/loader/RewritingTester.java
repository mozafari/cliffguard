package com.relationalcloud.tsqlparser.loader;

import java.util.ArrayList;
import com.relationalcloud.tsqlparser.Parser;

public class RewritingTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub



      
      //String sql = "SELECT max(*) FROM tab WHERE t=1";
      Schema schema = new Schema(null,"tpcc",null,null,null,null);
      SchemaTable t = new SchemaTable(schema.getSchemaName(), "tab");
      t.addColumn("a");
      t.addColumn("b");
      t.addColumn("c");
      t.addColumn("d");
      t.addColumn("tstart");
      t.addColumn("tend");
      schema.addTable(t);
    
      SchemaTable t2 = new SchemaTable(schema.getSchemaName(), "tab2");
      t2.addColumn("a");
      t2.addColumn("b");
      t2.addColumn("f");
      schema.addTable(t2);
    

      System.out.println("----------------------------------------------------------------------------------------------------");
      System.out.println(" THESE ARE UPDATES AND QUERIES REWRITTEN TO CREATE A HISTORY, AND TO PERFORM QUERIES AS-OF CURRENT_TIME");
      System.out.println("----------------------------------------------------------------------------------------------------");

      String sql = "INSERT INTO tab VALUES(1,1.27106114500000000e+09,'stringas')";
      System.out.println("BASE:" + sql);
      Parser p = new Parser("tpcc",schema, sql);
      String s = p.rewriteToTemporal().get(0);
      System.out.println("TEMPORAL:" + s);
      
      
      
      sql = "INSERT INTO tab(a,c) VALUES(1,'string')";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.rewriteToTemporal().get(0);
      System.out.println("TEMPORAL:" + s);

      sql = "INSERT INTO  tab (a) VALUES (?)";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.rewriteToTemporal().get(0);
      System.out.println("TEMPORAL:" + s);


      sql = "DELETE FROM tab WHERE b=\"7\"";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.rewriteToTemporal().get(0);
      System.out.println("TEMPORAL:" + s);

      sql = "UPDATE tab SET c=? WHERE a=?";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      ArrayList<String> li = p.rewriteToTemporal();
      
      
      System.out.println("TEMPORAL:" + li.get(0));
      System.out.println("TEMPORAL:" + li.get(1));

      sql = "SELECT * FROM tab t1, tab t2 WHERE c=7 AND t1.a=t2.a";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      li = p.rewriteToTemporal();
      System.out.println("TEMPORAL:" + li.get(0));
      
      sql = "SELECT * FROM tab";
      System.out.println("BASE:" + sql);
      p = new Parser("tpcc",schema, sql);
      li = p.rewriteToTemporal();
      System.out.println("TEMPORAL:" + li.get(0));
      
      System.out.println("----------------------------------------------------------------------------------------------------");
      System.out.println(" THESE ARE TEMPORAL QUERIES REWRITTEN TO NON-TEMPORAL SQL ON TUPLE-LEVEL-TIMESTAMPED DB");
      System.out.println("----------------------------------------------------------------------------------------------------");
      
      sql = "SELECT * FROM tab AS OF SYSTEM TIME 2003-01-01 00:00:00 WHERE c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);

      sql = "SELECT * FROM tab AS OF SYSTEM TIME CURRENT_TIME WHERE c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);

      
      sql = "SELECT * FROM tab  AS OF SYSTEM TIME 2003-01-01 00:00:00 AS t2, tab2 AS OF SYSTEM TIME 2004-01-01 00:00:00 WHERE t2.c=7 AND t2.a=tab2.a AND tab2.b=8";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);

      
      sql = "SELECT * FROM tab  VERSIONS AFTER SYSTEM TIME 2003-01-01 00:00:00 AS t2 WHERE t2.c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);
      
      sql = "SELECT * FROM tab VERSIONS BEFORE SYSTEM TIME 2003-01-01 00:00:00 AS t2 WHERE t2.c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);
      
      
      sql = "SELECT * FROM tab VERSIONS BETWEEN SYSTEM TIME 2003-01-01 00:00:00 AND SYSTEM TIME 2004-01-01 00:00:00 AS t2  WHERE t2.c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);

      
      
      sql = "SELECT * FROM tab VERSIONS BETWEEN SYSTEM TIME 2003-01-01 00:00:00 AND SYSTEM TIME 2004-01-01 00:00:00 AS t2  WHERE t2.c=7";
      System.out.println("TEMPORAL SQL:" + sql);
      p = new Parser("tpcc",schema, sql);
      s = p.toNonTemporalSQL();
      System.out.println("SIMPLE SQL:" + s);

      
      
      
     }

}
