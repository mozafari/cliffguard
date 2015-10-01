package edu.umich.robustopt.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.sql.Statement;
import java.sql.ResultSet;

import edu.umich.robustopt.physicalstructures.PhysicalStructure;

public class RecordedStatement {
	static private HashMap<PhysicalStructure, String> allDeployCommands = new HashMap<>();

	private Statement statement = null;
	private ArrayList<String> deployCommands = new ArrayList<String>();

	public RecordedStatement(Statement statement) {
		this.statement = statement;
	}

	public ResultSet executeQuery(String sql, Boolean isRecording) throws Exception {
		checkNotNull();
		ResultSet res = statement.executeQuery(sql);
		if (isRecording)
			deployCommands.add(sql);
		return res;
	}

	public Boolean execute(String sql, Boolean isRecording) throws Exception {
		checkNotNull();
		Boolean res = statement.execute(sql);
		if (isRecording)
			deployCommands.add(sql);
		return res;
	}

	public int executeUpdate(String sql, Boolean isRecording) throws Exception {
		checkNotNull();
		int res = statement.executeUpdate(sql);
		if (isRecording) 
			deployCommands.add(sql);
		return res;
	}

	public void close() throws Exception {
		checkNotNull();
		statement.close();
	}

	static public String getDeployCommands(PhysicalStructure physicalStructure) {
		return allDeployCommands.containsKey(physicalStructure) ? allDeployCommands.get(physicalStructure) : "";
	}

	public void finishDeploy(PhysicalStructure physicalStructure) {
		StringBuilder stringBuilder = new StringBuilder();
		for (String s : deployCommands) {
			stringBuilder.append(s + '\n');
		}
		allDeployCommands.put(physicalStructure, stringBuilder.toString());
	}

	private void checkNotNull() throws Exception{
		if (statement == null)
			throw new Exception("Statement in RecordedStatement must not be null");
	}
}
