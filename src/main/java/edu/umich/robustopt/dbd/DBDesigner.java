package edu.umich.robustopt.dbd;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;



import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public abstract class DBDesigner extends DBInvoker {
	
	public DBDesigner (LogLevel verbosity, DatabaseLoginConfiguration databaseLogin, ExperimentCache experimentCache) throws Exception {
		super(verbosity, databaseLogin, experimentCache);
		if (DatabaseLoginConfiguration.safeMode && !databaseLogin.evaluateEmptiness())
			throw new java.lang.Exception("You cannot design using a full database: " + databaseLogin);
	}


	public PhysicalDesign findDesignWithoutWeight(List<String> queries, DesignParameters designParameters) throws Exception {
		List<WeightedQuery> weightedQueries = new ArrayList<WeightedQuery>();
		for (int i=0; i<queries.size(); ++i)
			weightedQueries.add(new WeightedQuery(queries.get(i), 1.0));

		PhysicalDesign design = findDesignByWeightedQueries(weightedQueries, designParameters);
				
		return design;
	}
	

	public abstract PhysicalDesign findDesignByWeightedQueries(List<WeightedQuery> weightedQueries, DesignParameters designParameters) throws Exception;
	

	// produce some general statistics about the design process, e.g., number of actual designs, mins spent designing, etc.
	public abstract String reportStatistics();
		
}
