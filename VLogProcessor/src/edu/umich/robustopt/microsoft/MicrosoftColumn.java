package edu.umich.robustopt.microsoft;

import java.io.Serializable;

public class MicrosoftColumn implements Serializable {

	private static final long serialVersionUID = -662224284271098768L;
	private String columnName;
	private int columnId;
	private int isDescendingKey;
	private int keyOrder;
	private int partitionOrder;

	public MicrosoftColumn(String columnName, int columnId, int isDescendingKey, int keyOrder,
			int partitionOrder) {
		super();
		this.columnName = columnName;
		this.columnId = columnId;
		this.isDescendingKey = isDescendingKey;
		this.keyOrder = keyOrder;
		this.partitionOrder = partitionOrder;
	}

	public String getColumnName() {
		return columnName;
	}

	public int isDescendingKey() {
		return isDescendingKey;
	}

	public String toString() {
		return String.format("[name: %s, id: %d, is_descending_key: %d, key_ordinal: %d, partition_ordinal: %d]", 
				columnName, columnId, isDescendingKey, keyOrder, partitionOrder);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + columnId;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + isDescendingKey;
		result = prime * result + keyOrder;
		result = prime * result + partitionOrder;
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
		MicrosoftColumn other = (MicrosoftColumn) obj;
		if (columnId != other.columnId)
			return false;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (isDescendingKey != other.isDescendingKey)
			return false;
		if (keyOrder != other.keyOrder)
			return false;
		if (partitionOrder != other.partitionOrder)
			return false;
		return true;
	}
}
