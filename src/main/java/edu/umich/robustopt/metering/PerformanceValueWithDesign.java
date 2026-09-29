package edu.umich.robustopt.metering;

import java.util.List;


import edu.umich.robustopt.physicalstructures.*;

public class PerformanceValueWithDesign extends PerformanceValue {
	private static final long serialVersionUID = -2956875157231635887L;

	private PhysicalDesign design;
	
	public PerformanceValueWithDesign(String queryPlan, List<Long> allActualLatencies, List<Long> allOptimizerCosts, PhysicalDesign design) {
		super(queryPlan, allActualLatencies, allOptimizerCosts);
		this.design = design;
	}

	public PerformanceValueWithDesign(String queryPlan, List<Long> allOptimizerCosts, PhysicalDesign design) {
		super(queryPlan, allOptimizerCosts);
		this.design = design;
	}
	
	public PerformanceValueWithDesign(PerformanceValue performanceValue, PhysicalDesign design) {
		super(performanceValue.getQueryPlan(), performanceValue.getAllLatencies(), performanceValue.getAllOptimizerCosts());
		this.design = design;
	}

	public PhysicalDesign getDesign() {
		return design;
	}

	@Override
	public String toString() {
		return "avg latency=" + getMeanLatency()  	
				+ "\n**** design:\n" + PerformanceRecord.humanReadableDesign(design.getPhysicalStructures())
				+ "\n++++ query plan:\n" + queryPlan;
	}
}
