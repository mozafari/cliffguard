package edu.umich.robustopt.metering;

import java.io.Serializable;
import java.util.List;

import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.util.MyMathUtils;
import edu.umich.robustopt.util.Triple;
import edu.umich.robustopt.vertica.VerticaProjectionStructure;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;

public class PerformanceValue implements Serializable {
	private static final long serialVersionUID = 1765457892739896996L;
	protected String queryPlan;
	protected List<Long> allActualLatencies;
	protected List<Long> allOptimizerCosts;

	public PerformanceValue(String queryPlan, List<Long> allOptimizerCosts) {
		this.queryPlan = queryPlan;
		this.allActualLatencies = null;
		this.allOptimizerCosts = allOptimizerCosts;
	}
	
	public PerformanceValue(String queryPlan, List<Long> allActualLatencies, List<Long> allOptimizerCosts) {
		this.queryPlan = queryPlan;
		this.allActualLatencies = allActualLatencies;
		this.allOptimizerCosts = allOptimizerCosts;
	}

	public boolean hasActualLatency() {
		return (this.allActualLatencies != null);
	}
	
	public String getQueryPlan() {
		return queryPlan;
	}

	public List<Long> getAllLatencies() {
		if (hasActualLatency())
			return allActualLatencies;
		else
			return allOptimizerCosts;
	}
	
	public List<Long> getAllActualLatencies() {
		return allActualLatencies;
	}
	
	public List<Long> getAllOptimizerCosts() {
		return allOptimizerCosts;
	}
	
	@Override
	public String toString() {
		String summary = (hasActualLatency() ? "avg actual latency=" + getMeanActualLatency() : "no actual latency");
		summary += (", avg optimizer cost=" + getMeanOptimizerCost() + "\n++++ query plan:\n" + queryPlan);
		return summary;
	}
	
	public Triple<Integer, Integer, Integer> compareWinTieLoss(PerformanceValue otherValue) throws Exception {
		if ((hasActualLatency() != otherValue.hasActualLatency())
				|| 
			(getAllLatencies().size() != otherValue.getAllLatencies().size())
			)
				throw new Exception("Cannot compare different sizes " + 
						hasActualLatency() + " and " + otherValue.hasActualLatency() + "("+
						(getAllLatencies().size() + " and " + otherValue.getAllLatencies().size()) + ")");				

		int wins=0, ties=0, loss=0;
		for (int i=0; i<getAllLatencies().size(); ++i) {
			if (getAllLatencies().get(i) < otherValue.getAllLatencies().get(i))
				++wins;
			else if (getAllLatencies().get(i) > otherValue.getAllLatencies().get(i))
				++loss;
			else
				++ties;
		}

		return new Triple<Integer, Integer, Integer>(wins, ties, loss);
	}
	
	public List<String> getProjectionNamesUsedInThePlan(QueryPlanParser queryPlanParser) {
		return queryPlanParser.searchForPhysicalStructureNamesInCanonicalExplainOutput(queryPlan);
	}

	public List<String> getPhysicalStructureBaseNamesUsedInThePlan(QueryPlanParser queryPlanParser) {
		return queryPlanParser.searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(queryPlan);		
	}

	public long getMeanLatency() {
		return MyMathUtils.getMeanLongs(getAllLatencies());
	}

	public long getMinLatency() {
		return MyMathUtils.getMinLongs(getAllLatencies());
	}
	
	public long getMaxLatency() {
		return MyMathUtils.getMaxLongs(getAllLatencies());
	}
	
	public long getStdLatency() {
		return MyMathUtils.getStdLongs(getAllLatencies());
	}
	
	public long getMeanActualLatency() {
		return MyMathUtils.getMeanLongs(allActualLatencies);
	}

	public long getMinActualLatency() {
		return MyMathUtils.getMinLongs(allActualLatencies);
	}
	
	public long getMaxActualLatency() {
		return MyMathUtils.getMaxLongs(allActualLatencies);
	}
	
	public long getStdActualLatency() {
		return MyMathUtils.getStdLongs(allActualLatencies);
	}
	
	public long getMeanOptimizerCost() {
		return MyMathUtils.getMeanLongs(allOptimizerCosts);
	}

	public long getMinOptimizerCost() {
		return MyMathUtils.getMinLongs(allOptimizerCosts);
	}
	
	public long getMaxOptimizerCost() {
		return MyMathUtils.getMaxLongs(allOptimizerCosts);
	}
	
	public long getStdOptimizerCost() {
		return MyMathUtils.getStdLongs(allOptimizerCosts);
	}
	
}
