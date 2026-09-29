package edu.umich.robustopt.workloads;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

public class WideTableWorkloadGenerator extends SyntheticDistributionDistancePairWorkloadGenerator {
	public static final String defaultDatabaseAlias = "wide";

	private static Map<String, Schema> WIDE_SCHEMAS = null; 
	
	public WideTableWorkloadGenerator(ConstantValueManager cvm, String DBVendorLoginType) throws Exception {
		super(loadSchema(DBVendorLoginType), cvm);
	}

	public WideTableWorkloadGenerator(String dbAlias, String DBVendorLoginType) throws Exception {
		this(new ConstantValueManager(dbAlias, 0.05, (String)null, DBVendorLoginType), DBVendorLoginType);
	}

	public WideTableWorkloadGenerator(String DBVendorLoginType) throws Exception {
		this(defaultDatabaseAlias, DBVendorLoginType);
	}
	
	public WideTableWorkloadGenerator(String dbAlias, double sample, File constantMngrFile, String DBVendorLoginType) throws Exception {
		this(ConstantValueManager.RestoreStateFromFile(dbAlias, sample, constantMngrFile, null, DBVendorLoginType), DBVendorLoginType);
	}

	private static Map<String, Schema> loadSchema(String DBVendorLoginType) throws Exception {
		if (WIDE_SCHEMAS == null) {
			WIDE_SCHEMAS = SchemaUtils.GetSchemaMapFromDefaultSources(defaultDatabaseAlias, DBVendorLoginType).getSchemas();
		}
		return WIDE_SCHEMAS;
	}
	
	@Override
	protected Query_SWGO GenerateRandomQuery() {
		Schema s = ListUtils.PickOne(new ArrayList<Schema>(schema.values()));
		SchemaTable t = ListUtils.PickOne(new ArrayList<SchemaTable>(s.getTables()));
		List<ColumnDescriptor> whereCols = chooseExactlyKColumns(t, 2);
		List<ColumnDescriptor> selectCols = new ArrayList<ColumnDescriptor>();
		selectCols.add(whereCols.get(0));
 		List<ColumnDescriptor> emptyCols = Collections.emptyList();
 		Query_SWGO q = null;
 		try {
			q = new Query_SWGO(selectCols, emptyCols, whereCols, emptyCols, emptyCols);
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return q;
	}

	public static void main(String[] args) throws Exception {
		//createPowerLawFrequencies(10, 35);

		final DistributionDistancePair[] D_VALUES = array(
				new DistributionDistancePair(0.1, 0.3, 1),
				new DistributionDistancePair(0.1, 0.5, 1),
				new DistributionDistancePair(0.1, 0.7, 1));
		File dir1 = new File(GlobalConfigurations.RO_BASE_PATH, "dataset_wide");
		dir1.mkdirs();
		
		int NumOfClustersPerWindow = 10;
		int NumOfQueriesPerWindow = 35;
		
		boolean useDummyDistibutions = false;
		
		for (int d_id = 0; d_id < D_VALUES.length; d_id++) {
			File dir = new File(GlobalConfigurations.RO_BASE_PATH, "dataset_wide/d" + d_id + "-" + D_VALUES[d_id].toString());
			dir.mkdirs();
			WideTableWorkloadGenerator gen;
			if (useDummyDistibutions)
				gen = new WideTableWorkloadGenerator(new TestingConstantValueManager(), VerticaDatabaseLoginConfiguration.class.getSimpleName());
			else {
				gen = new WideTableWorkloadGenerator(VerticaDatabaseLoginConfiguration.class.getSimpleName());			
			}
			
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
