package edu.umich.robustopt.workloads;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;

import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.clustering.SqlLogFileManager;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.ListUtils;
import edu.umich.robustopt.util.SchemaUtils;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public class SimpleTPCHSyntheticWorkloadGenerator extends SyntheticDistributionDistancePairWorkloadGenerator {
	public static final String defaultDatabaseAlias = "tpch";
	
	private static final Map<String, Schema> TPCH_SCHEMAS = SchemaUtils.GetTPCHSchema();
	
	public SimpleTPCHSyntheticWorkloadGenerator(ConstantValueManager cvm) {
		super(TPCH_SCHEMAS, cvm);
	}

	public SimpleTPCHSyntheticWorkloadGenerator(String dbAlias, String DBVendor) throws Exception {
		this(new ConstantValueManager(dbAlias, 0.05, null, null));
	}

	public SimpleTPCHSyntheticWorkloadGenerator(String DBVendor) throws Exception {
		this(defaultDatabaseAlias, DBVendor);
	}
	
	public SimpleTPCHSyntheticWorkloadGenerator(String dbAlias, double sample, File constantMngrFile, String DBVendor) throws Exception {
		this(ConstantValueManager.RestoreStateFromFile(dbAlias, sample, constantMngrFile, null, DBVendor));
	}
	
	protected Query_SWGO GenerateRandomQuery() {
		// pick the number of predicates to go in the where clause
		// sample table uniformly at random:
		Schema s = ListUtils.PickOne(new ArrayList<Schema>(schema.values()));
		SchemaTable t = ListUtils.PickOne(new ArrayList<SchemaTable>(s.getTables()));
		List<ColumnDescriptor> whereCols = chooseUptoKColumns(t, 1);
		List<ColumnDescriptor> groupBy = chooseUptoKColumns(t, 2);
		List<ColumnDescriptor> selectCols = chooseUptoKColumns(t, 1);
		List<ColumnDescriptor> emptyCols = Collections.emptyList();
		Query_SWGO q = null;
		try {
			q = new Query_SWGO(MergeDescriptors(selectCols, groupBy), emptyCols, whereCols, groupBy, emptyCols);
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return q;
	}

	private static List<ColumnDescriptor> MergeDescriptors(
			List<ColumnDescriptor> a,
			List<ColumnDescriptor> b) {
		Set<ColumnDescriptor> x = new HashSet<ColumnDescriptor>();
		x.addAll(a);
		x.addAll(b);
		return new ArrayList<ColumnDescriptor>(x);
	}


	public static void main(String[] args) throws Exception {
		//createPowerLawFrequencies(10, 35);

		final DistributionDistancePair[] D_VALUES = array(
				new DistributionDistancePair(0.1, 0.3, 1),
				new DistributionDistancePair(0.1, 0.5, 1),
				new DistributionDistancePair(0.1, 0.7, 1));
		File dir1 = new File(GlobalConfigurations.RO_BASE_PATH, "dataset_synthetic_tpch");
		dir1.mkdirs();
		
		int NumOfClustersPerWindow = 10;
		int NumOfQueriesPerWindow = 35;
		
		boolean useDummyDistibutions = false;
		String DBVendor = VerticaDatabaseLoginConfiguration.class.getSimpleName();
		
		for (int d_id = 0; d_id < D_VALUES.length; d_id++) {
			File dir = new File(GlobalConfigurations.RO_BASE_PATH, "dataset_synthetic_tpch/d" + d_id + "-" + D_VALUES[d_id].toString());
			dir.mkdirs();
			SimpleTPCHSyntheticWorkloadGenerator gen;
			if (useDummyDistibutions)
				gen = new SimpleTPCHSyntheticWorkloadGenerator(new TestingConstantValueManager());
			else
				gen = new SimpleTPCHSyntheticWorkloadGenerator(DBVendor);
			
			DistributionDistancePair curDistancePair = D_VALUES[d_id];
			
			List<List<String>> sqlWindows = new ArrayList<List<String>>();
			ClusteredWindow window = null;
			for (int w = 0; w < 10; w++) {
				if (w==0)
					window = gen.GenerateRandomWindow(NumOfClustersPerWindow, NumOfQueriesPerWindow, FrequencyDistribution.PowerLaw);
				else
					window = gen.forecastNextWindow(window, curDistancePair);
				System.out.println("win" + w + ": " + window);
				List<String> sqlQueries = window.getAllSql();
				sqlWindows.add(sqlQueries);
			}
			SqlLogFileManager.writeListOfListOfQueriesToSeparateFiles(dir, sqlWindows);
		}
		
		System.out.println("Queries were generated successfully.");
	}

	
	
}
