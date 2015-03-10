package com.relationalcloud.tsqlparser;

public class ParserHacks {

  /**
   * This class contains the hacks that makes the parser digest SQL. Eventually
   * the parser will be improved to avoid all of this.
   * 
   * @param sql
   * @return
   */
  public static String fixSql(String sql) {

    // TEMPORARY HACK, REMOVE PORTION OF SQL THAT THE PARSER CANNOT MANAGE
    // AND ARE NOT STRICTLY NEEDED FOR THE PROJECT
	  
	sql = sql.replaceAll("FORCE INDEX\\(PRIMARY\\)", "");
    sql = sql.replaceAll("force index\\(primary\\)", "");
    
    return sql;
  }

}
