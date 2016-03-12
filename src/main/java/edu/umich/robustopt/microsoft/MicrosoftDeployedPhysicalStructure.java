package edu.umich.robustopt.microsoft;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class MicrosoftDeployedPhysicalStructure extends DeployedPhysicalStructure {

	private String tableOrViewName;
	private boolean isDisabled;

	public MicrosoftDeployedPhysicalStructure(String schema, String name,
			String basename, PhysicalStructure structure) {
		super(schema, name, basename, structure);
	}

	public MicrosoftDeployedPhysicalStructure(String schema, String name,
			String basename, String tableOrViewName, PhysicalStructure structure, boolean isDisabled) {
		super(schema, name, basename, structure);
		this.tableOrViewName = tableOrViewName;
		this.isDisabled = isDisabled;
	}

	public String getTableOrViewName() {
		return tableOrViewName;
	}

	public boolean isDisabled() {
		return isDisabled;
	}

	public boolean disableStructure(Connection conn) throws CloneNotSupportedException {
		PhysicalStructure structure = this.getStructure();
		if (structure instanceof MicrosoftIndex) {
			MicrosoftIndex index = (MicrosoftIndex)structure;
			if (index.disable(conn)) {
				isDisabled = true;
				return true;
			}
		} else if (structure instanceof MicrosoftIndexedView) {
			MicrosoftIndexedView indexedView = (MicrosoftIndexedView)structure;
			if (indexedView.disable(conn)) {
				isDisabled = true;
				return true;
			}
		}
		return false;
	}

	public boolean enableStructure(Connection conn) throws CloneNotSupportedException {
		PhysicalStructure structure = this.getStructure();
		if (structure instanceof MicrosoftIndex) {
			MicrosoftIndex index = (MicrosoftIndex)structure;
			if (index.enable(conn)) {
				isDisabled = false;
				return true;
			}
		} else if (structure instanceof MicrosoftIndexedView) {
			MicrosoftIndexedView indexedView = (MicrosoftIndexedView)structure;
			if (indexedView.enable(conn)) {
				isDisabled = false;
				return true;
			}
		}
		return false;
	}
	
	public static MicrosoftDeployedPhysicalStructure getDeployedDesignedStructureByNameFromDB(Connection conn, String name) throws Exception {
		Statement stmt = conn.createStatement();

		// check indexes first.
		String sql = "SELECT t.name as table_name, s.name as schema_name, ind.* " +
				"FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id " +
				"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
				"WHERE t.is_ms_shipped = 0 AND ind.type = 2 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0 and ind.is_hypothetical = 0 AND " +
				"ind.name = '" + name + "';";

		ResultSet rs = stmt.executeQuery(sql);

		if (rs.next()) {

			String indexName = rs.getString("name");
			String schemaName = rs.getString("schema_name");
			String tableName = rs.getString("table_name");

			int objectId = rs.getInt("object_id");
			int indexId = rs.getInt("index_id");
			int indexType = rs.getInt("type");

			int isPrimaryKey = rs.getInt("is_primary_key");
			int isDisabled = rs.getInt("is_disabled");
			int isUnique = rs.getInt("is_unique");
			int isUniqueConstraint = rs.getInt("is_unique_constraint");

			List<MicrosoftColumn> keyColumnList = new ArrayList<MicrosoftColumn>();
			List<MicrosoftColumn> includeColumnList = new ArrayList<MicrosoftColumn>();

			String sqlGetKeyColumns = "SELECT c.name as column_name, ic.* FROM sys.index_columns ic "
        			+ "INNER JOIN sys.columns c ON ic.object_id = c.object_id and ic.column_id = c.column_id "
        			+ "WHERE ic.object_id = ? and ic.index_id = ? and ic.is_included_column = 0 " +
        			"ORDER BY key_ordinal";
			PreparedStatement keyColumnStatement = conn.prepareStatement(sqlGetKeyColumns);
			keyColumnStatement.setInt(1, objectId);
			keyColumnStatement.setInt(2, indexId);
			ResultSet keyColumns = keyColumnStatement.executeQuery();

			while (keyColumns.next()) {
				MicrosoftColumn newColumn = new MicrosoftColumn(keyColumns.getString("column_name"), 
						keyColumns.getInt("column_id"), keyColumns.getInt("is_descending_key"), 
						keyColumns.getInt("key_ordinal"), keyColumns.getInt("partition_ordinal"));
				keyColumnList.add(newColumn);
			}
			keyColumns.close();

			String sqlGetIncludeColumns = "SELECT c.name as column_name, ic.* FROM sys.index_columns ic "
        			+ "INNER JOIN sys.columns c ON ic.object_id = c.object_id and ic.column_id = c.column_id "
        			+ "WHERE ic.object_id = ? and ic.index_id = ? and ic.is_included_column = 1";
			PreparedStatement includeColumnStatement = conn.prepareStatement(sqlGetIncludeColumns);
			includeColumnStatement.setInt(1, objectId);
			includeColumnStatement.setInt(2, indexId);
			ResultSet includeColumns = includeColumnStatement.executeQuery();
			while (includeColumns.next()) {
				MicrosoftColumn newColumn = new MicrosoftColumn(includeColumns.getString("column_name"),
						includeColumns.getInt("column_id"), includeColumns.getInt("is_descending_key"), 
						includeColumns.getInt("key_ordinal"), includeColumns.getInt("partition_ordinal"));
				includeColumnList.add(newColumn);
			}
			includeColumns.close();

			MicrosoftIndex newIndex = new MicrosoftIndex(objectId, indexName, schemaName, tableName, indexType, indexId, isUnique, 
					isPrimaryKey, isUniqueConstraint, isDisabled, keyColumnList, includeColumnList);

			stmt.close();

			return new MicrosoftDeployedPhysicalStructure(schemaName, indexName, indexName, tableName, newIndex, (isDisabled==1));
		}
		rs.close();

		// check indexed views.
		stmt = conn.createStatement();
		sql = "SELECT o.object_id, o.name as view_name, i.*, s.name as schema_name " +
				"FROM sys.objects o " +
				"INNER JOIN sys.indexes i ON o.object_id = i.object_id " +
				"INNER JOIN sys.views v ON v.object_id = i.object_id " +
				"INNER JOIN sys.schemas s ON s.schema_id = v.schema_id " +
				"WHERE o.type = 'V' AND i.type = 1 AND i.name = '" + name + "'";
		rs = stmt.executeQuery(sql);
		
		if (rs.next()) {

			String viewName = rs.getString("view_name");
			String indexName = rs.getString("name");
			String schemaName = rs.getString("schema_name");

			int objectId = rs.getInt("object_id");
			int indexId = rs.getInt("index_id");
			int indexType = rs.getInt("type");

			int isPrimaryKey = rs.getInt("is_primary_key");
			int isDisabled = rs.getInt("is_disabled");
			int isUnique = rs.getInt("is_unique");
			int isUniqueConstraint = rs.getInt("is_unique_constraint");

			List<String> columns = new ArrayList<String>();
			List<String> aliases = new ArrayList<String>();
			List<Integer> indexedColumns = new ArrayList<Integer>();
			List<Integer> isIndexedColumnsDescending = new ArrayList<Integer>();

			String from = "";
			String where = "";
			String groupBy = "";

			String sqlGetViewDefinition = "SELECT definition from sys.sql_modules WHERE object_id = ?";
			PreparedStatement viewDefinitionStmt = conn.prepareStatement(sqlGetViewDefinition);
			viewDefinitionStmt.setInt(1, objectId);

			ResultSet viewDefinition = viewDefinitionStmt.executeQuery();
			if (viewDefinition.next()) {

				String definition = viewDefinition.getString("definition");
				TGSqlParser parser = new TGSqlParser(EDbVendor.dbvmssql);
				parser.setSqltext(definition);
				int ret = parser.parse();

				if (ret == 0) {
	 				if (parser.sqlstatements.size() != 1) {
	 					throw new Exception("More than one statements in view definition");
					}
					else {
						TCustomSqlStatement customSqlStatement = parser.sqlstatements.get(0);

						if (customSqlStatement instanceof TCreateViewSqlStatement) {

							TCreateViewSqlStatement viewStatement = (TCreateViewSqlStatement)customSqlStatement;
							TSelectSqlStatement selectSqlStatement = viewStatement.getSubquery();

							// get select columns
							for (int i = 0; i < selectSqlStatement.getResultColumnList().size(); ++i) {
								TResultColumn column = selectSqlStatement.getResultColumnList().getResultColumn(i);

								String columnExpr = column.getExpr().toString();
								String alias = "";
								if (column.getAliasClause() != null) {
									alias = column.getAliasClause().toString();
								}

								columns.add(columnExpr);
								aliases.add(alias);
							}

							// get from clause
							from = selectSqlStatement.joins.toString();

							// get where clause
							TWhereClause whereClause = selectSqlStatement.getWhereClause();
							if (whereClause != null) {
								where = whereClause.toString();
							}

							// get group-by & having clause
							TGroupBy groupByClause = selectSqlStatement.getGroupByClause();
							if (groupByClause != null) {
								groupBy = groupByClause.toString();
							}

							// get indexed columns
							String sqlGetIndexedColumns = "SELECT c.name, i.is_descending_key FROM sys.index_columns i " +
									"INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = i.column_id " +
									"WHERE i.object_id = ? AND i.index_id = ? ORDER BY key_ordinal";

							PreparedStatement getIndexedColumnStatement = conn.prepareStatement(sqlGetIndexedColumns);
							getIndexedColumnStatement.setInt(1, objectId);
							getIndexedColumnStatement.setInt(2, indexId);

							ResultSet indexedColumnSet = getIndexedColumnStatement.executeQuery();

							// add indexed columns
							while (indexedColumnSet.next())
							{
								String indexColumnName = indexedColumnSet.getString("name");
								int isDescending = indexedColumnSet.getInt("is_descending_key");
								for (int i = 0; i < columns.size(); ++i) {
									String col = columns.get(i);
									String alias = aliases.get(i);

									// check alias if exists.
									if (alias != "") {
										if (alias.trim().compareTo(indexColumnName.trim()) == 0) {
											indexedColumns.add(i);
											break;
										}
									}
									else {
										if (col.contains(indexColumnName)) {
											indexedColumns.add(i);
											break;
										}
									}
								}
								isIndexedColumnsDescending.add(isDescending);

								if (indexedColumns.size() != isIndexedColumnsDescending.size()) {
									throw new Exception("Indexed column in an indexed view not found: " + indexColumnName);
								}
							}

							indexedColumnSet.close();
						}
						else {
							throw new Exception("Invalid view definition");
						}
					}
				}
			}
        	viewDefinition.close();

        	MicrosoftIndexedView newView = new MicrosoftIndexedView(columns, aliases, indexedColumns, isIndexedColumnsDescending,
        			from, where, groupBy, viewName, indexName, schemaName, objectId, indexId, indexType, isDisabled, isUnique, 
        			isPrimaryKey, isUniqueConstraint);

        	stmt.close();

			return new MicrosoftDeployedPhysicalStructure(schemaName, indexName, indexName, viewName, newView, (isDisabled==1));
		}

		stmt.close();

		return null;
	}

	@Override
	public DeployedPhysicalStructure clone() {
		return new MicrosoftDeployedPhysicalStructure(getSchema(), getName(), getBasename(), getTableOrViewName(), 
				this.getStructure(), isDisabled());
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

		MicrosoftIndex i1 = new MicrosoftIndex(1, "index", "schema", "table", 2, 1, 0, 0, 0, 0, keyCols1, incCols1);
		MicrosoftIndex i2 = new MicrosoftIndex(1, "index", "schema", "table", 2, 1, 0, 0, 0, 0, keyCols2, incCols2);

		MicrosoftDeployedPhysicalStructure s1 = new MicrosoftDeployedPhysicalStructure("s", "n1", "b", "t", i1, false);
		MicrosoftDeployedPhysicalStructure s2 = new MicrosoftDeployedPhysicalStructure("s", "n2", "b", "t", i2, false);

		Set<DeployedPhysicalStructure> aSet = new HashSet<DeployedPhysicalStructure>();
		aSet.add(s1);
		if (aSet.contains(s2)) {
			System.out.println("set contains s2: expected");
		} else {
			System.out.println("set does not contain s2: unexpected");
		}
	}
}
