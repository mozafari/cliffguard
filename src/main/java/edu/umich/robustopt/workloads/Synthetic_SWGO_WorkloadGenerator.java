package edu.umich.robustopt.workloads;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.expression.ScalarExpressionUtils;
import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.common.Randomness;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.ListUtils;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.StringUtils;

public abstract class Synthetic_SWGO_WorkloadGenerator<D extends DistributionDistance> extends Query_SWGO_WorkloadGenerator<D> {
	
	public Synthetic_SWGO_WorkloadGenerator(Map<String, Schema> schema, ConstantValueManager constManager) {
		super(schema, constManager);
	}
	
	public Synthetic_SWGO_WorkloadGenerator(String dbAlias, String databaseLoginFile, String DBVendor) throws Exception {
		super(dbAlias, databaseLoginFile, DBVendor);
	}
	
	public Synthetic_SWGO_WorkloadGenerator(Map<String, Schema> schema, String dbAlias, double samplingRate, File f) throws Exception {
		super(schema, dbAlias, samplingRate, f);
	}
	
	/*
	@Override
	public void setDistributionDistance(DistributionDistancePair distance) throws Exception {
		this.distancePair = distance;
	}*/
	
	// performs the changes in-place, and achieves the new frequency by creating random sql instances that have the same "query" structure as the original one
	@Override
	public Cluster createClusterWithNewFrequency(Cluster cluster, int newFreq) throws Exception {
		assert newFreq >= 0;
			
		if (cluster.getFrequency() == 0)
			throw new Exception("0 freq cluster, have no examples to create instances of.");
			
		Query_SWGO query = (Query_SWGO)(cluster.retrieveAQueryAtPosition(0));
		
		List<String> sqlQueries = new ArrayList<String>();	
		for (int i=0; i<newFreq; ++i) {
			String sql = createRandomSqlInstanceForQuery(query);
			sqlQueries.add(sql);
		}
			
		List<Query_SWGO> queries = getQueryParser().convertSqlListToQuery(sqlQueries, getSchemaMap());
		List<Query> pureQueries = Query.convertToListOfQuery(queries);
		Cluster newCluster = new Cluster(pureQueries);
		return newCluster;
		}
	
	protected String createRandomSqlInstanceForQuery(Query_SWGO query) throws Exception {
		// we only support one query of the format:
		// SELECT group_key1, ..., group_keyN, min(proj1), ..., min(projN)
		// FROM table1, ..., tableN
		// WHERE predicate1 < const1 AND predicate2 < const2 AND ... AND predicateN < constN
		// GROUP BY group_key1, ..., group_keyN
		// LIMIT 1000
		// where each constK is computed by taking a value which captures 80% of the values
		
		List<String> tableNames = new ArrayList<String>();
		for (NamedIdentifier ni : query.extractTables()) {
			tableNames.add(ni.getQualifiedName());
		}

		List<String> wherePredicates = new ArrayList<String>();
		for (ColumnDescriptor cd : query.getWhere()) {
			ValueDistribution vd = constManager.getColumnDistribution(cd);
			if (vd == null)
				throw new RuntimeException("vd null");
			Object konst = vd.cdf(0.8);
			wherePredicates.add(cd.getColumnName() + " <= " + ScalarExpressionUtils.ConstructFrom(konst).toString());
		}

		List<String> selectProjections = new ArrayList<String>();
		for (ColumnDescriptor cd : query.getGroup_by()) {
			selectProjections.add(cd.getColumnName());
		}

		Set<ColumnDescriptor> groupKeys = new HashSet<ColumnDescriptor>(query.getGroup_by());
		for (ColumnDescriptor cd : query.getSelect()) {
			if (groupKeys.contains(cd))
				continue;
			selectProjections.add("min(" + cd.getColumnName() + ")");
		}

		List<String> gbKeys = new ArrayList<String>();
		for (ColumnDescriptor cd : query.getGroup_by()) {
			gbKeys.add(cd.getColumnName());
		}

		List<String> orderByColumns = new ArrayList<String>();
		for (ColumnDescriptor cd : query.getOrder_by()) 
			orderByColumns.add(cd.getColumnName());
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		if (selectProjections.isEmpty())
			sb.append(" 1 ");
		else
			sb.append(StringUtils.Join(selectProjections, ", "));
		
		sb.append(" FROM ");
		sb.append(StringUtils.Join(tableNames, ", "));
		
		if (!wherePredicates.isEmpty()) {
			sb.append(" WHERE ");
			sb.append(StringUtils.Join(wherePredicates, " AND "));
		}

		if (!gbKeys.isEmpty()) {
			sb.append(" GROUP BY ");	
			sb.append(StringUtils.Join(gbKeys, ", "));
		}

		if (!orderByColumns.isEmpty()) {
			sb.append(" ORDER BY ");	
			sb.append(StringUtils.Join(orderByColumns, ", "));
		}
		
		sb.append(" LIMIT 10;");

		return sb.toString();
	}
	
	protected abstract Query_SWGO GenerateRandomQuery();

	protected List<ColumnDescriptor> chooseUptoKColumns(SchemaTable table, int upperLimit) {
		int nColsTotal = upperLimit <= 0 ? table.getNumColumns() : upperLimit;
		int nColsWhere = Randomness.randGen.nextInt(nColsTotal) + 1;
		List<String> cols = PickWithoutReplacement(table.getColumns(), nColsWhere);
		List<ColumnDescriptor> cds = new ArrayList<ColumnDescriptor>();
		for (String c : cols)
			cds.add(new ColumnDescriptor(table.getSchemaName(), table.getTableName(), c));
		return cds;
	}

	protected List<ColumnDescriptor> chooseExactlyKColumns(SchemaTable table, int exactNumberOfColumns) {
		int nColsTotal = exactNumberOfColumns > table.getNumColumns() ? table.getNumColumns() : exactNumberOfColumns;
		List<String> cols = PickWithoutReplacement(table.getColumns(), nColsTotal);
		List<ColumnDescriptor> cds = new ArrayList<ColumnDescriptor>();
		for (String c : cols)
			cds.add(new ColumnDescriptor(table.getSchemaName(), table.getTableName(), c));
		return cds;
	}
	
	protected static <T> T[] array(T... elems) {
		return elems;
	}
	
	protected <T> List<T> PickWithoutReplacement(List<T> elems, int n) {
		return ListUtils.PickWithoutReplacement(elems, n);
	}
	
	public void saveStateToFile(File f) throws IOException {
		this.constManager.saveDataDistributionsToFile(f);
	}
}
