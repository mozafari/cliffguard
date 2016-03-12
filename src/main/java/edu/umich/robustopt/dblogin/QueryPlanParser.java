package edu.umich.robustopt.dblogin;

import java.io.Serializable;
import java.util.List;

public abstract class QueryPlanParser implements Serializable {
	private static final long serialVersionUID = -6488968704418897878L;

	public abstract List<String> searchForPhysicalStructureBaseNamesInRawExplainOutput(String rawExplainOutput);

	public abstract String extractCanonicalQueryPlan(String rawQueryPlan);
	
	public abstract List<String> searchForPhysicalStructureNamesInCanonicalExplainOutput(String queryPlan);

	public abstract List<String> searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(String queryPlan);

	public abstract long extractTotalCostsFromRawPlan(String string);

	public abstract List<String> searchForPhysicalStructureNamesInRawExplainOutput(String rawExplainOutput);
	 
}
