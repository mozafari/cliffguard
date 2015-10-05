package edu.umich.robustopt.microsoft;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.umich.robustopt.physicalstructures.Index;
import edu.umich.robustopt.common.RecordedStatement;

public class MicrosoftIndex extends Index implements Serializable {

	private static final long serialVersionUID = -6829722097970667031L;
	public static final int TYPE_HEAP = 0;
	public static final int TYPE_CLUSTERED = 1;
	public static final int TYPE_NONCLUSTERED = 2;
	public static final int TYPE_XML = 3;
	public static final int TYPE_SPATIAL = 4;
	public static final int TYPE_CLUSTERED_COLUMNSTORE = 5;
	public static final int TYPE_NONCLUSTERED_COLUMNSTORE = 6;
	public static final int TYPE_NONCLUSTERED_HASH = 7;

	private int objectId;
	private String indexName;
	private String schemaName;
	private String tableName;
	private int indexType;
	private int indexId;

	private int isUnique;
	private int isPrimaryKey;
	private int isUniqueConstraint;
	private int isDisabled;

	List<MicrosoftColumn> keyColumns;
	List<MicrosoftColumn> includeColumns;

	public MicrosoftIndex(int objectId, String indexName, String schemaName,
			String tableName, int indexType, int indexId, int isUnique,
			int isPrimaryKey, int isUniqueConstraint, int isDisabled) {
		super();
		this.objectId = objectId;
		this.indexName = indexName;
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.indexType = indexType;
		this.indexId = indexId;
		this.isUnique = isUnique;
		this.isPrimaryKey = isPrimaryKey;
		this.isUniqueConstraint = isUniqueConstraint;
		this.isDisabled = isDisabled;

		keyColumns = new ArrayList<MicrosoftColumn>();
		includeColumns = new ArrayList<MicrosoftColumn>();
	}

	public MicrosoftIndex(int objectId, String indexName, String schemaName,
			String tableName, int indexType, int indexId, int isUnique,
			int isPrimaryKey, int isUniqueConstraint, int isDisabled,
			List<MicrosoftColumn> keyColumns,
			List<MicrosoftColumn> includeColumns) {
		super();
		this.objectId = objectId;
		this.indexName = indexName;
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.indexType = indexType;
		this.indexId = indexId;
		this.isUnique = isUnique;
		this.isPrimaryKey = isPrimaryKey;
		this.isUniqueConstraint = isUniqueConstraint;
		this.isDisabled = isDisabled;
		this.keyColumns = keyColumns;
		this.includeColumns = includeColumns;
	}

	public String getIndexName() {
		return indexName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public int isDisabled() {
		return isDisabled;
	}

	@Override
	public String getHumanReadableSummary() {
		String summary = String.format("INDEX: %s ON %s.%s, type: %d, is_disabled: %d\n", 
				indexName, schemaName, tableName, indexType, isDisabled);
		if (keyColumns.size() > 0)
		{
			summary += "\tKey Columns:\n";
			for (MicrosoftColumn column : keyColumns)
			{
				summary += "\t\t" + column.toString() + "\n";
			}
		}
		if (includeColumns.size() > 0)
		{
			summary += "\tInclude Columns:\n";
			for (MicrosoftColumn column : includeColumns)
			{
				summary += "\t\t" + column.toString() + "\n";
			}
		}
		return summary;
	}

	public boolean disable(Connection conn) {
		String disableIndexSql = String.format("ALTER INDEX %s ON %s.%s DISABLE", indexName, schemaName, tableName);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(disableIndexSql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean enable(Connection conn) {
		String disableIndexSql = String.format("ALTER INDEX %s ON %s.%s REBUILD", indexName, schemaName, tableName);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(disableIndexSql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean deploy(Connection conn) throws Exception {
		return deploy(conn, false);
	}
	
	@Override
	public ArrayList<String> createPhysicalStructureSQL(String structureName) throws Exception {
		String createIndexSql = "CREATE";

		// UNIQUE
		if (isUnique == 1) {
			createIndexSql += " UNIQUE";
		}

		// CLUSTERED/NONCLUSTERED
		if (indexType == TYPE_CLUSTERED)
			createIndexSql += " CLUSTERED";
		else if (indexType == TYPE_NONCLUSTERED)
			createIndexSql += " NONCLUSTERED";
		else
			throw new Exception("Unsupported index type: " + indexType);

		createIndexSql += " INDEX";
		createIndexSql += " " + indexName + " ON " + schemaName + "." + tableName;
		createIndexSql += "\n(\n";

		// add key columns
		for (int i = 0; i < keyColumns.size(); ++i) {
			MicrosoftColumn column = keyColumns.get(i);
			if (i == keyColumns.size() - 1) {
				createIndexSql += column.getColumnName();
				if (column.isDescendingKey() == 0) {
					createIndexSql += " ASC\n";
				} else {
					createIndexSql += " DESC\n";
				}
			} else {
				createIndexSql += column.getColumnName();
				if (column.isDescendingKey() == 0) {
					createIndexSql += " ASC,\n";
				} else {
					createIndexSql += " DESC,\n";
				}
			}
		}
		createIndexSql += ")";

		// add include columns
		if (includeColumns.size() > 0) {
			createIndexSql += "\nInclude(\n";
			for (int i = 0; i < includeColumns.size(); ++i) {
				MicrosoftColumn column = includeColumns.get(i);
				if (i == includeColumns.size() - 1) {
					createIndexSql += column.getColumnName();
				} else {
					createIndexSql += column.getColumnName() + ",\n";
				}
			}
			createIndexSql += ")";
		}

		//default options
		createIndexSql += " WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]"; 
		
		ArrayList<String> res = new ArrayList<String>();
		res.add(createIndexSql);
		return res;
	}
	
	
	public boolean deploy(Connection conn, boolean debug) throws Exception {
		String createIndexSql = createPhysicalStructureSQL("").get(0);
		try
		{
			RecordedStatement rstmt = new RecordedStatement(conn.createStatement());
			rstmt.executeUpdate(createIndexSql, true);
			rstmt.close();
			rstmt.finishDeploy(this);
		} catch (SQLException e) {
			System.out.println("Failed to deploy an index with following statement:\n" + createIndexSql);
			e.printStackTrace();
			return false;
		}
		return true;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + indexType;
		result = prime * result + isPrimaryKey;
		result = prime * result + isUnique;
		result = prime * result + isUniqueConstraint;
		result = prime * result + objectId;
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
		MicrosoftIndex other = (MicrosoftIndex) obj;
		if (includeColumns == null) {
			if (other.includeColumns != null)
				return false;
		} else if (other.includeColumns != null) {
			if (includeColumns.size() != other.includeColumns.size())
				return false;
			for (MicrosoftColumn column : includeColumns) {
				if (!other.includeColumns.contains(column))
					return false;
			}
		} else 
			return false;
		if (indexType != other.indexType)
			return false;
		if (isPrimaryKey != other.isPrimaryKey)
			return false;
		if (isUnique != other.isUnique)
			return false;
		if (isUniqueConstraint != other.isUniqueConstraint)
			return false;
		if (keyColumns == null) {
			if (other.keyColumns != null)
				return false;
		} else if (other.keyColumns != null) {
			if (keyColumns.size() != other.keyColumns.size())
				return false;
			else {
				for (int i=0;i<keyColumns.size();++i)
				{
					MicrosoftColumn c1 = keyColumns.get(i);
					MicrosoftColumn c2 = other.keyColumns.get(i);
					if (!c1.equals(c2)) {
						return false;
					}
				}
			}
		} else 
			return false;
		if (objectId != other.objectId)
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

	public static void main(String[] args) {
		MicrosoftColumn c1 = new MicrosoftColumn("col1", 1,0,0,0);
		MicrosoftColumn c2 = new MicrosoftColumn("col2", 2,0,0,0);
		MicrosoftColumn d1 = new MicrosoftColumn("col1", 1,0,0,0);
		MicrosoftColumn d2 = new MicrosoftColumn("col2", 2,0,0,0);

		List<MicrosoftColumn> keyCols1 = new ArrayList<MicrosoftColumn>();
		keyCols1.add(c1);
		List<MicrosoftColumn> keyCols2 = new ArrayList<MicrosoftColumn>();
		keyCols2.add(d1);
		List<MicrosoftColumn> incCols1 = new ArrayList<MicrosoftColumn>();
		incCols1.add(c2);
		List<MicrosoftColumn> incCols2 = new ArrayList<MicrosoftColumn>();
		incCols2.add(d2);

		MicrosoftIndex i1 = new MicrosoftIndex(1, "index123", "schema", "table", 2, 1, 0, 0, 0, 0, keyCols1, incCols1);
		MicrosoftIndex i2 = new MicrosoftIndex(1, "index1414", "schema", "table", 2, 1, 0, 0, 0, 0, keyCols2, incCols2);

		if (i1.equals(i2)) {
			System.out.println("two indexes are equal: CORRECT");
		}
	}
	
}
