package edu.umich.robustopt.dblogin;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ResiliantConnection implements Connection {
	private String connectionPhrase;
	private Connection actualConnection;
	private int timeoutInSecs;
	
	public ResiliantConnection(String connectionPhrase, int timeoutInSeconds) throws SQLException {
		this.actualConnection = DriverManager.getConnection(connectionPhrase);
		this.connectionPhrase = connectionPhrase;
		this.timeoutInSecs = timeoutInSeconds;
	}

	public void check() throws SQLException {
		try {
			Statement s = actualConnection.createStatement();
			s.execute("SELECT 1");
			s.close();
		} catch (SQLException e) {
			System.out.println("Lost connection, reconnecting ... ");
			actualConnection = DriverManager.getConnection(connectionPhrase);
		}
	}
	
	public void clearWarnings() throws SQLException {
		check();
		actualConnection.clearWarnings();
	}

	public void close() throws SQLException {
		check();
		actualConnection.close();
	}

	public void commit() throws SQLException {
		check();
		actualConnection.commit();
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		check();
		return actualConnection.createArrayOf(typeName, elements);
	}

	public Blob createBlob() throws SQLException {
		check();
		return actualConnection.createBlob();
	}

	public Clob createClob() throws SQLException {
		check();
		return actualConnection.createClob();
	}

	public NClob createNClob() throws SQLException {
		check();
		return actualConnection.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		check();
		return actualConnection.createSQLXML();
	}

	public Statement createStatement() throws SQLException {
		check();
		return actualConnection.createStatement();
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		check();
		return actualConnection.createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		check();
		return actualConnection.createStatement(resultSetType,
				resultSetConcurrency);
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		check();
		return actualConnection.createStruct(typeName, attributes);
	}

	public boolean getAutoCommit() throws SQLException {
		check();
		return actualConnection.getAutoCommit();
	}

	public String getCatalog() throws SQLException {
		check();
		return actualConnection.getCatalog();
	}

	public Properties getClientInfo() throws SQLException {
		check();
		return actualConnection.getClientInfo();
	}

	public String getClientInfo(String name) throws SQLException {
		check();
		return actualConnection.getClientInfo(name);
	}

	public int getHoldability() throws SQLException {
		check();
		return actualConnection.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		check();
		return actualConnection.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException {
		check();
		return actualConnection.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		check();
		return actualConnection.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException {
		check();
		return actualConnection.getWarnings();
	}

	public boolean isClosed() throws SQLException {
		check();
		return actualConnection.isClosed();
	}

	public boolean isReadOnly() throws SQLException {
		check();
		return actualConnection.isReadOnly();
	}

	public boolean isValid(int timeout) throws SQLException {
		return actualConnection.isValid(timeout);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		check();
		return actualConnection.isWrapperFor(iface);
	}

	public String nativeSQL(String sql) throws SQLException {
		check();
		return actualConnection.nativeSQL(sql);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		check();
		return actualConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		check();
		return actualConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		check();
		return actualConnection.prepareCall(sql);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		check();
		return actualConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		check();
		return actualConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		check();
		return actualConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		check();
		return actualConnection.prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		check();
		return actualConnection.prepareStatement(sql, columnNames);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		check();
		return actualConnection.prepareStatement(sql);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		check();
		actualConnection.releaseSavepoint(savepoint);
	}

	public void rollback() throws SQLException {
		check();
		actualConnection.rollback();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		check();
		actualConnection.rollback(savepoint);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		check();
		actualConnection.setAutoCommit(autoCommit);
	}

	public void setCatalog(String catalog) throws SQLException {
		check();
		actualConnection.setCatalog(catalog);
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		actualConnection.setClientInfo(properties);
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		actualConnection.setClientInfo(name, value);
	}

	public void setHoldability(int holdability) throws SQLException {
		check();
		actualConnection.setHoldability(holdability);
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		check();
		actualConnection.setReadOnly(readOnly);
	}

	public Savepoint setSavepoint() throws SQLException {
		check();
		return actualConnection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		check();
		return actualConnection.setSavepoint(name);
	}

	public void setTransactionIsolation(int level) throws SQLException {
		check();
		actualConnection.setTransactionIsolation(level);
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		check();
		actualConnection.setTypeMap(map);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		check();
		return actualConnection.unwrap(iface);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub
	}

	public int getNetworkTimeout() {
		return timeoutInSecs;
	}
	
	
}
