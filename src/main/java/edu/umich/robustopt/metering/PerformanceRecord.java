package edu.umich.robustopt.metering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math.stat.StatUtils;


import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.SafeMap;
import edu.umich.robustopt.vertica.VerticaProjectionStructure;
import edu.umich.robustopt.vertica.VerticaQueryPlanParser;

public class PerformanceRecord implements Serializable {
	static BLog log = new BLog(LogLevel.DEBUG);
	
	private static final long serialVersionUID = -1265717743267688111L;	
	
	private String query;
	/*
	 * all times are measured and reported in MILI SECONDS
	 */
	private Map<String, PerformanceValueWithDesign> performanceValueWithDesigns = new SafeMap<String, PerformanceValueWithDesign>();
	private boolean finished = false;
	
	public PerformanceRecord(String query) {
		this.query = query;
	}

	public boolean isDone() {
		return finished;
	}
	
	public void setDone() {
		this.finished = true;
	}
	
	public PerformanceValueWithDesign getPerformanceValueWithDesign(String designAlgorithmName) {
		if (performanceValueWithDesigns.containsKey(designAlgorithmName))
			return performanceValueWithDesigns.get(designAlgorithmName);
		else {
			log.error("Key not found: " + designAlgorithmName);
			return null;
		}
	}
	
	public Set<String> getAllDesignAlgorithmNames() {
		return new HashSet<String>(performanceValueWithDesigns.keySet());
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("query= " + query + "\n");
		for (String name : performanceValueWithDesigns.keySet())
			buf.append("Name: " + name + ":\n" + performanceValueWithDesigns.get(name) + "\n");
		return buf.toString();
	}
	
	public static String humanReadableDesign(Set<PhysicalStructure> design) {
		StringBuilder summary = new StringBuilder();
		for (PhysicalStructure p : design)
			summary.append(p.getHumanReadableSummary() + "\n");
		return summary.toString();
	}

	public void record(String designAlgorithmName, PerformanceValueWithDesign performanceValueWithDesign) {
		if (performanceValueWithDesigns.containsKey(designAlgorithmName))
			System.err.println("repeated key: " + designAlgorithmName + " overriding old value");
		performanceValueWithDesigns.put(designAlgorithmName, performanceValueWithDesign);
	}
	
	public String getQuery() {
		return this.query;
	}

	//TODO: This function currently only works for Vertica!! We need to allow for other DBs
	public void checkIfPlansAreLegal(Map<String, PhysicalStructure> nameToStruct, QueryPlanParser queryPlanParser) throws CloneNotSupportedException {
		for (String algorithmName : performanceValueWithDesigns.keySet())	{
			PerformanceValueWithDesign performanceValueWithDesign = performanceValueWithDesigns.get(algorithmName);
			List<String> usedProjNames = queryPlanParser.searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(performanceValueWithDesign.getQueryPlan());
			for (String structName : usedProjNames) { 
				PhysicalStructure usedStruct = nameToStruct.get(structName);
				if (!performanceValueWithDesign.getDesign().getPhysicalStructures().contains(usedStruct)) {
					System.err.println("Algorithm:" + algorithmName + " illegally used projection: " + structName + " with struct: " + usedStruct.getHumanReadableSummary() + " while the only allowed ones were :");
					//for (VerticaProjectionStructure allowedStruct : performanceValue.getDesign())
					//	System.err.println("\t " + allowedStruct.getHumanReadableSummary());
				}
			}
		}
	}	
	

}
