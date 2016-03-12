package edu.umich.robustopt.staticanalysis;

import java.io.Serializable;

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

	@Override
	public String toString() {
		return "ColumnDescriptor [" + schemaName + "." + tableName + "." + columnName + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnDescriptor other = (ColumnDescriptor) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
	
}
