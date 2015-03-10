package com.relationalcloud.tsqlparser.loader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


public class TestSchemaLoader {

  public static void main(String[] args) {

    Properties ini = new Properties();
    try {
      ini.load(new FileInputStream(System.getProperty("prop")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Register jdbcDriver
    try {
      Class.forName(ini.getProperty("driver"));
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(ini.getProperty("conn")+"information_schema", ini
          .getProperty("user"), ini.getProperty("password"));
    
    Schema s = SchemaLoader.loadSchemaFromDB(conn,"tpcc2");
  
    System.out.println(s.toStringWithMetadata());
    }catch(Exception e){
      
      e.printStackTrace();
      
    }
  }
  
  
}
