package edu.umich.robustopt.microsoft;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.umich.robustopt.common.RecordedStatement;
import edu.umich.robustopt.physicalstructures.IndexedView;

public class MicrosoftIndexedView extends IndexedView implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3192493509810980728L;
	private List<String> columns;
	private List<String> aliases;
	private List<Integer> indexedColumns;
	private List<Integer> isDescending;

	private String from;
	private String where;
	private String groupBy;

	private String viewName;
	private String indexName;
	private String schemaName;

	private int objectId;
	private int indexId;
	private int indexType;

	private int isDisabled;
	private int isUnique;
	private int isPrimaryKey;
	private int isUniqueConstraint;



	public MicrosoftIndexedView(List<String> columns, List<String> aliases,
			List<Integer> indexedColumns, List<Integer> isDescending,
			String from, String where, String groupBy, String viewName,
			String indexName, String schemaName, int objectId, int indexId,
			int indexType, int isDisabled, int isUnique, int isPrimaryKey,
			int isUniqueConstraint) {
		super();
		this.columns = columns;
		this.aliases = aliases;
		this.indexedColumns = indexedColumns;
		this.isDescending = isDescending;
		this.from = from;
		this.where = where;
		this.groupBy = groupBy;
		this.viewName = viewName;
		this.indexName = indexName;
		this.schemaName = schemaName;
		this.objectId = objectId;
		this.indexId = indexId;
		this.indexType = indexType;
		this.isDisabled = isDisabled;
		this.isUnique = isUnique;
		this.isPrimaryKey = isPrimaryKey;
		this.isUniqueConstraint = isUniqueConstraint;
	}

	public MicrosoftIndexedView() {
		// TODO Auto-generated constructor stub
	}

	public String getIndexName() {
		return indexName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getViewName() {
		return viewName;
	}

	public int isDisabled() {
		return isDisabled;
	}

	@Override
	public String getHumanReadableSummary() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean disable(Connection conn) {
		String disableIndexSql = String.format("ALTER INDEX %s ON %s.%s DISABLE", indexName, schemaName, viewName);
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
		String disableIndexSql = String.format("ALTER INDEX %s ON %s.%s REBUILD", indexName, schemaName, viewName);
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

	public boolean deploy(Connection conn) throws Exception{
		return deploy(conn, false);
	}
	
	@Override
	public ArrayList<String> createPhysicalStructureSQL(String structureName) throws Exception {
		String createViewSql = "CREATE VIEW";
		createViewSql += " " + schemaName + "." + viewName + " WITH SCHEMABINDING";
		createViewSql += " AS\n";
		createViewSql += "SELECT ";

		for (int i = 0; i < columns.size(); ++i) {
			String column = columns.get(i);
			String alias = aliases.get(i);
			
			createViewSql += " " + column;
			if (!alias.isEmpty()) {
				createViewSql += " as ";
				createViewSql += alias;
			}
			if (i < columns.size() - 1) {
				createViewSql += ",";
			}
		}
		createViewSql += "\nFROM " + from + "\n" + where + "\n" + groupBy;

		String createIndexSql = "CREATE";

		// UNIQUE
		if (isUnique == 1) {
			createIndexSql += " UNIQUE";
		}

		// CLUSTERED/NONCLUSTERED
		if (indexType == MicrosoftIndex.TYPE_CLUSTERED) {
			createIndexSql += " CLUSTERED";
		} else if (indexType == MicrosoftIndex.TYPE_NONCLUSTERED) {
			createIndexSql += " NONCLUSTERED";
		} else {
			throw new Exception("Unsupported index type: " + indexType);
		}

		createIndexSql += " INDEX";
		createIndexSql += " " + indexName + " ON " + schemaName + "." + viewName;
		createIndexSql += "\n(\n";

		// add key columns
		for (int i = 0; i < indexedColumns.size(); ++i) {
			int columnIndex = indexedColumns.get(i);
			String column = columns.get(columnIndex);
			String alias = aliases.get(columnIndex);
			if (i == indexedColumns.size() - 1) {
				if (!alias.isEmpty()) {
					createIndexSql += alias;
				} else {
					createIndexSql += column;
				}

				if (isDescending.get(i) == 0) {
					createIndexSql += " ASC\n";
				} else {
					createIndexSql += " DESC\n";
				}
			} else {
				if (!alias.isEmpty()) {
					createIndexSql += alias;
				} else {
					createIndexSql += column;
				}

				if (isDescending.get(i) == 0) {
					createIndexSql += " ASC,\n";
				} else {
					createIndexSql += " DESC,\n";
				}
			}
		}
		createIndexSql += ")";
		//default options
		createIndexSql += " WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]";
		
		ArrayList<String> res = new ArrayList<String>();
		res.add(createViewSql);
		res.add(createIndexSql);
		return res;
	}

	public boolean deploy(Connection conn, boolean debug) throws Exception{
		String requiredSetOptions = "SET ARITHABORT ON\n" +
				"SET CONCAT_NULL_YIELDS_NULL ON\n" +
				"SET QUOTED_IDENTIFIER ON\n" +
				"SET ANSI_NULLS ON\n" +
				"SET ANSI_PADDING ON\n" +
				"SET ANSI_WARNINGS ON\n" +
				"SET NUMERIC_ROUNDABORT OFF";

		ArrayList<String> createIndexedViewSql = createPhysicalStructureSQL("");

		String dropViewIfExists = "IF EXISTS (SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_name = '" + viewName + "') BEGIN\n" +
				"DROP VIEW " + schemaName + "." + viewName + "\n END";

		if (debug) {
			System.out.println("DROP VIEW IF EXISTS Statement: \n" + dropViewIfExists);
		}

		try {
			RecordedStatement rstmt = new RecordedStatement(conn.createStatement());
			rstmt.executeUpdate(requiredSetOptions, true);
			rstmt.executeUpdate(dropViewIfExists, true);
			rstmt.executeUpdate(createIndexedViewSql.get(0), true);
			rstmt.executeUpdate(createIndexedViewSql.get(1), true);
			rstmt.close();
			rstmt.finishDeploy(this);
		} catch (SQLException e) {
			System.out.println("Failed to deploy an indexed view with following statements: \n" + requiredSetOptions + "\n" + createIndexedViewSql.get(0) + "\n" + createIndexedViewSql.get(1));
			e.printStackTrace();
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (String c : columns) {
			result = prime * result + c.hashCode();
		}
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + ((groupBy == null) ? 0 : groupBy.hashCode());
		result = prime * result + indexType;
		result = prime * result + isPrimaryKey;
		result = prime * result + isUnique;
		result = prime * result + isUniqueConstraint;
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result + ((where == null) ? 0 : where.hashCode());
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
		MicrosoftIndexedView other = (MicrosoftIndexedView) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (other.columns != null) {
			for (int i = 0; i < columns.size();++i) {
				String col1 = columns.get(i);
				String col2 = other.columns.get(i);
				if (!col1.equals(col2)) {
					return false;
				}
			}
		}
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
		if (groupBy == null) {
			if (other.groupBy != null)
				return false;
		} else if (!groupBy.equals(other.groupBy))
			return false;
		if (indexType != other.indexType)
			return false;
		if (indexedColumns == null) {
			if (other.indexedColumns != null)
				return false;
		} else if (other.indexedColumns != null) {
			if (indexedColumns.size() != other.indexedColumns.size()) {
				return false;
			}
			for (int i = 0;i < indexedColumns.size();++i) {
				String col1 = columns.get(indexedColumns.get(i));
				String col2 = other.columns.get(other.indexedColumns.get(i));
				if (!col1.equals(col2)) {
					return false;
				}
			}
		} else 
			return false;
		if (isDescending == null) {
			if (other.isDescending != null)
				return false;
		} else if (other.isDescending != null) {
			if (isDescending.size() != other.isDescending.size()) {
				return false;
			}
			for (int i = 0; i < isDescending.size(); ++i) {
				int val1 = isDescending.get(i).intValue();
				int val2 = other.isDescending.get(i).intValue();
				if (val1 != val2) {
					return false;
				}
			}
		} else
			return false;
		if (isPrimaryKey != other.isPrimaryKey)
			return false;
		if (isUnique != other.isUnique)
			return false;
		if (isUniqueConstraint != other.isUniqueConstraint)
			return false;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (where == null) {
			if (other.where != null)
				return false;
		} else if (!where.equals(other.where))
			return false;
		return true;
	}

}
