package com.relationalcloud.tsqlparser.statement.rename;

import java.util.HashMap;

import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveRewriterVisitor;
import com.relationalcloud.tsqlparser.visitors.recursive.RecursiveVisitor;


/**
 * A RENAME TABLE statement
 * <br>Syntax:
 * <br>RENAME TABLE tbl_name_1 TO new_tbl_name_1 (, table_name_2 TO new_table_name_2)*
 * @author fangar
 *
 */
public class RenameTable implements Statement{

	//A Map with the old table and the new table name
	private HashMap<Table, String> tableRenames;

	public HashMap<Table, String> getTableRenames() {
		return tableRenames;
	}

	public void setTableRenames(HashMap<Table, String> tableRenames) {
		this.tableRenames = tableRenames;
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void accept(RecursiveVisitor recursiveVisitor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object accept(RecursiveRewriterVisitor recursiveRewriterVisitor) {
		// TODO Auto-generated method stub
		return null;
	}
	

	
}
