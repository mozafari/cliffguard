package edu.umich.robustopt.staticanalysis;

import java.io.Serializable;

import org.apache.commons.lang3.SystemUtils;
import edu.umich.robustopt.util.NamedIdentifier;

public class ColumnDescriptor implements Serializable, Cloneable {
	private static final long serialVersionUID = 4597432850836393063L;

	private final String schemaName;
	private final String tableName;
	private final String columnName;
	
	public ColumnDescriptor(String schemaName, String tableName,
			String columnName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.columnName = columnName;
	}

	@Override
	public ColumnDescriptor clone() throws CloneNotSupportedException {
		return new ColumnDescriptor(schemaName, tableName, columnName);
	}
	
	public NamedIdentifier getTableFullName() {
		return new NamedIdentifier(schemaName, tableName);
	}
	
	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public String getQualifiedTableName() {
		return schemaName + "." + tableName;
	}
	
	public String getQualifiedName() {
		return schemaName + "." + tableName + "." + columnName;
	}

	private String getColumnCaseSensitiveName() {
		if (columnName==null) return null;
		//if (SystemUtils.IS_OS_WINDOWS)
		// item 5.2.21 in ANSI, ISO, IEC, SQL standard shows that a name for
		// table or column should be normalized to all-uppercase form
		// during resolution.
		return columnName.toUpperCase();
		//else if (SystemUtils.IS_OS_LINUX)
		//	return columnName;
		//assert false;
		//return null;
	}

	private String getTableCaseSensitiveName() {
		if (tableName==null) return null;
		//if (SystemUtils.IS_OS_WINDOWS)
		return tableName.toUpperCase();
		//else if (SystemUtils.IS_OS_LINUX)
		//	return tableName;
		//assert false;
		//return null;
	}
	@Override
	public String toString() {
		return "ColumnDescriptor [" + schemaName + "." + tableName + "." + columnName + "]";
	}

	@Override
	public int hashCode() {

		String columnString = getColumnCaseSensitiveName();
		String tableString = getTableCaseSensitiveName();
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (columnString==null? 0 : columnString.hashCode());
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result
				+ (tableString==null? 0 : tableString.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO two private inner class ColumnNameString and TableNameString would be helpful
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnDescriptor other = (ColumnDescriptor) obj;

		String columnString = getColumnCaseSensitiveName();
		String tableString = getTableCaseSensitiveName();
		String otherColumnString = other.getColumnCaseSensitiveName();
		String otherTableString = other.getTableCaseSensitiveName();

		if (columnString == null) {
			if (otherColumnString != null)
				return false;
		} else if (!columnString.equals(otherColumnString))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (tableString == null) {
			if (otherTableString != null)
				return false;
		} else if (!tableString.equals(otherTableString))
			return false;
		return true;
	}
	
}
