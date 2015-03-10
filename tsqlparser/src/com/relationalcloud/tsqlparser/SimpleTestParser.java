package com.relationalcloud.tsqlparser;


import java.util.ArrayList;
import java.util.Vector;

import com.relationalcloud.tsqlparser.Parser;
import com.relationalcloud.tsqlparser.expression.BinaryExpression;
import com.relationalcloud.tsqlparser.loader.ForeignKey;
import com.relationalcloud.tsqlparser.loader.PrimaryKey;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.visitors.WhereConditionForTableVisitor;

public class SimpleTestParser {

  /**
   * @param args
   */
  public static void main(String[] args) {

    try {
      
      //String sql = " INSERT INTO tab VALUES(1,1.27106114500000000e+09,'stringas')";
      //String sql = "CREATE TABLE A (a1 int, a2 varchar(255));";
      //String sql = "INSERT INTO `revision` (rev_id,rev_page,rev_text_id,rev_comment,rev_minor_edit,rev_user,rev_user_text,rev_timestamp,rev_deleted,rev_len,rev_parent_id) VALUES (NULL,'2483524',182358929,'','0','0',\"10.1.26.116\",\"1110494025\",'0','20278','182356676')";
    	//String sql = "INSERT INTO `tab` VALUES (1,2,3,4);";
    	//String sql = "SELECT a FROM tab,tob WHERE b=21 AND tab.a = tob.b";
    	String sql = "SELECT * FROM (select * from tab where a=10) as niha WHERE b=21";

    	
      Schema schema = new Schema(null,"tpcc",null,null,null,null);
      SchemaTable t = new SchemaTable("public", "tab");
      t.addColumn("a");
      t.addColumn("b");
      t.addColumn("c");
      t.addColumn("d");
      schema.addTable(t);
      Vector<String> v =new Vector<String>();
      v.add("c");
      t.addConstraint(new PrimaryKey("tab",v));
      
      
      SchemaTable t2 = new SchemaTable("public", "tob");
      t2.addColumn("a");
      t2.addColumn("b");
      t2.addColumn("f");
      schema.addTable(t2);
      Vector<String> v3 =new Vector<String>();
      v3.add("a");
      t2.addConstraint(new PrimaryKey("tob",v3));
    
      
      Parser p = new Parser("tpcc",schema, sql);
      // LIST TABLES INVOLVED IN THE QUERY
      System.out.println("TABLES: " + p.getTableStringList());
      System.out.println("COUNT  QUERY: " + p.getCountEquivalent());
      //System.out.println("PRIMARY KEY  QUERY postJoin: " + p.getPrimaryKeyEquivalent(true));
      //System.out.println("PRIMARY KEY  QUERY preJoin: " + p.getPrimaryKeyEquivalent(false));
      //System.out.println("SELECTed columns: "+p.getCountEquivalent());
      
      
		WhereConditionForTableVisitor v2 = new WhereConditionForTableVisitor();
		ArrayList<BinaryExpression> exp2 = v2.getWhereForTableCondition(p.stmt,"tab", schema);
      
      System.out.println("WHERE: " + exp2);
  
           
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
