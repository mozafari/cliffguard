package edu.umich.robustopt.common;

import java.sql.Statement;
import java.sql.ResultSet;

import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class RecordedStatement {
	private Statement statement = null;
	private PhysicalStructure physicalStructure = null;

	public RecordedStatement(Statement statement, PhysicalStructure physicalStructure) {
		this.statement = statement;
		this.physicalStructure = physicalStructure;
	}

	public ResultSet executeQuery(String sql) throws Exception {
		checkNotNull();
		ResultSet res = statement.executeQuery(sql);
		physicalStructure.writeToDeployCommands(sql);
		return res;
	}

	public Boolean execute(String sql) throws Exception {
		checkNotNull();
		Boolean res = statement.execute(sql);
		physicalStructure.writeToDeployCommands(sql);
		return res;
	}

	public int executeUpdate(String sql) throws Exception {
		checkNotNull();
		int res = statement.executeUpdate(sql);
		physicalStructure.writeToDeployCommands(sql);
		return res;
	}

	public void close() throws Exception {
		checkNotNull();
		statement.close();
	}

	private void checkNotNull() throws Exception{
		if (statement == null || physicalStructure == null)
			throw new Exception("Statement and PhysicalStructure in RecordedStatement cannot be null");
	}
}
