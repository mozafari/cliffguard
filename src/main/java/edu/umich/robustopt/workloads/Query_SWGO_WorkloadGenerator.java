package edu.umich.robustopt.workloads;

import java.io.File;
import java.util.Map;

import com.relationalcloud.tsqlparser.loader.Schema;

import edu.umich.robustopt.clustering.Clustering;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.QueryParser;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.util.SchemaUtils;

public abstract class Query_SWGO_WorkloadGenerator<D extends DistributionDistance> extends WorkloadGenerator<D, Query_SWGO> {
	protected final Map<String, Schema> schema;
	protected final ConstantValueManager constManager;
	
	public Query_SWGO_WorkloadGenerator(Map<String, Schema> schema, ConstantValueManager constManager) {
		this.schema = schema;
		this.constManager = constManager;
	}
	
	public Query_SWGO_WorkloadGenerator(String dbName, String databaseLoginFile, String DBVendor) throws Exception {
		this(SchemaUtils.GetSchemaMapFromDefaultSources(dbName, DBVendor).getSchemas(), new ConstantValueManager(dbName, 0.05, databaseLoginFile, DBVendor));
	}
	
	public Query_SWGO_WorkloadGenerator(Map<String, Schema> schema, String dbName, double samplingRate, File constantMngrFile) throws Exception {
		this(schema, ConstantValueManager.RestoreStateFromFile(dbName, samplingRate, constantMngrFile, null, null));
	}
	
	@Override
	public QueryParser<Query_SWGO> getQueryParser() {
		return new Query_SWGO.QParser();
	}

	@Override
	public Map<String, Schema> getSchemaMap() {
		return schema;
	}
	
	@Override
	public Clustering getClustering() {
		return new Clustering_QueryEquality();
	}
}
