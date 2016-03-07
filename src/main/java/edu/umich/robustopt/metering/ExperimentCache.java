package edu.umich.robustopt.metering;

import java.io.BufferedOutputStream;
/*
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
*/
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
/*
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
*/
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.robustopt.algorithms.DesignAlgorithm;
import edu.umich.robustopt.algorithms.RobustDesigner;
import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.metering.DesignKey;
import edu.umich.robustopt.metering.PerformanceValue;
import edu.umich.robustopt.microsoft.MicrosoftQueryPlanParser;
import edu.umich.robustopt.vertica.*;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.SafeMap;
import edu.umich.robustopt.util.Timer;

public class ExperimentCache implements Serializable {
	private static final long serialVersionUID = -2521010019252997632L;
	private String original_filename = null;
	public int synchronizationFrequencyForPerformanceRecords = 1;
	private int unsyncedPerformanceRecords = 0;
	public int synchronizationFrequencyForDesigns = 1;
	private int unsyncedDesigns = 0;
	public int synchronizationFrequencyForDeployedPhysicalStructures = 1;
	private int unsyncedDeployedPhysicalStructures = 0;
	private transient String newFileName = null;
	// private transient CacheWriter cacheWriter = null;
	private transient long designsHit;
	private transient long designsMissed;
	private transient long verbatimPerformanceHits;
	private transient long verbatimPerformanceMisses;
	private transient long planBasedPerformanceHits;
	private transient long planBasedPerformanceMisses;
	private transient long explainCommandHits;
	private transient long explainCommandMisses;
	private transient long secondsSpentLoadingFromFile;
	private transient long secondsSpentWritingToFile;
	private transient long numberOfWriteOperationsToFile;
	
	private SafeMap<DesignKey, PhysicalDesign> designCache = new SafeMap<DesignKey, PhysicalDesign>();
	private SafeMap<DesignKey, List<DesignExecutionTrace>> designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>(); 
	private SafeMap<PerformanceKey, PerformanceValue> performanceCache = new SafeMap<PerformanceKey, PerformanceValue>();
	private SafeMap<PerformanceKey, String> explainCache = new SafeMap<PerformanceKey, String>();	
	
	//Below are members that contain information about "deployed" structures!
	private SafeMap<PhysicalStructure, Double> physicalStructureToDiskSizeInGigabytes = new SafeMap<PhysicalStructure, Double>();
	private SafeMap<PhysicalStructure, String> physicalStructureToDeployedNames = new SafeMap<PhysicalStructure, String>();
	private SafeMap<String, PhysicalStructure> deployedStructureNamesToStructures = new SafeMap<String, PhysicalStructure>();
	private SafeMap<String, DeployedPhysicalStructure> deployedStructureNamesToDeployedStructures = new SafeMap<String, DeployedPhysicalStructure>();
	private QueryPlanParser queryPlanParser;
	
	public ExperimentCache(String filename, int synchronizationFrequencyForPerformanceRecords, int synchronizationFrequencyForDesigns, int synchronizationFrequencyForDeployedPhysicalStructures, QueryPlanParser queryPlanParser) throws Exception {
		assert(synchronizationFrequencyForDesigns>=1 && synchronizationFrequencyForPerformanceRecords>=1 && synchronizationFrequencyForDeployedPhysicalStructures>=1);

		this.original_filename = filename;
		this.synchronizationFrequencyForPerformanceRecords = synchronizationFrequencyForPerformanceRecords;
		this.synchronizationFrequencyForDesigns = synchronizationFrequencyForDesigns;
		this.synchronizationFrequencyForDeployedPhysicalStructures = synchronizationFrequencyForDeployedPhysicalStructures;
		this.queryPlanParser = queryPlanParser;
		// DY : removed colon.. Windows does not allow colons in filename.
		String dateStr = new SimpleDateFormat("yyyy-MM-dd_HHmm").format(new Date());
		this.newFileName = original_filename + "." + dateStr;
		
		this.designsHit = 0;
		this.designsMissed = 0;
		this.verbatimPerformanceHits = 0;
		this.verbatimPerformanceMisses = 0;
		this.planBasedPerformanceHits = 0;
		this.planBasedPerformanceMisses = 0;
		this.explainCommandHits = 0;
		this.explainCommandMisses = 0;
		this.secondsSpentLoadingFromFile = 0;
		this.secondsSpentWritingToFile = 0;
		this.numberOfWriteOperationsToFile = 0;
	}

	public SafeMap<PhysicalStructure, Double> getPhysicalStructure() {
		return physicalStructureToDiskSizeInGigabytes;
	}



	public SafeMap<PhysicalStructure, String> getPhysicalStructureToDeployedNames() {
		return physicalStructureToDeployedNames;
	}



	public SafeMap<String, PhysicalStructure> getDeployedPhysicalStructureBaseNamesToStructures() {
		return deployedStructureNamesToStructures;
	}



	public SafeMap<String, DeployedPhysicalStructure> getDeployedPhysicalStructureNamesToDeployedPhysicalStructures() {
		return deployedStructureNamesToDeployedStructures;
	}


	/** each call to the following method will override all the keys in the map, but that's ok because they are the same 
	 * @throws IOException 
	 * @throws FileNotFoundException */
	public void cacheDeployedPhysicalStructures(Set<DeployedPhysicalStructure> deployedStructures) throws Exception {
		for (DeployedPhysicalStructure deployedStructure : deployedStructures) {
			PhysicalStructure structure = deployedStructure.getStructure();
			String structureBaseName = deployedStructure.getBasename();
			Double diskSize = deployedStructure.getDiskSizeInGigabytes();
			physicalStructureToDiskSizeInGigabytes.put(structure, diskSize);
			physicalStructureToDeployedNames.put(structure, structureBaseName);
			deployedStructureNamesToStructures.put(structureBaseName, structure);
			deployedStructureNamesToDeployedStructures.put(structureBaseName, deployedStructure);
		}
		++unsyncedDeployedPhysicalStructures;
		if (unsyncedDeployedPhysicalStructures >= synchronizationFrequencyForDeployedPhysicalStructures) {
			saveTheEntireCache();
			unsyncedDeployedPhysicalStructures = 0;
		}
	}
	
	public void removeReferencesToStaleStructures() {
		int stalePerformanceEntries=0, staleExplainEntries = 0;
		Set<String> stalePhysicalStructureBaseNames = new HashSet<String>();
		
		//first remove all references to performance entries that mention a stale structure
		Set<PerformanceKey> keysToRemove = new HashSet<PerformanceKey>();
		for (PerformanceKey key : performanceCache.keySet()) {
			PerformanceValue value = performanceCache.get(key);
			List<String> structureBaseNamesUsed = value.getPhysicalStructureBaseNamesUsedInThePlan(queryPlanParser);
			boolean isStale = false;
			for (String structureBaseName : structureBaseNamesUsed)
				if (!deployedStructureNamesToDeployedStructures.containsKey(structureBaseName)) {
					isStale = true;
					stalePhysicalStructureBaseNames.add(structureBaseName);
					break;
				}
			if (isStale) {
				keysToRemove.add(key);
				++ stalePerformanceEntries;
			}
		}
		for (PerformanceKey key : keysToRemove)
			performanceCache.remove(key);

		// now remove stale entries in the explain cache
		keysToRemove.clear();
		for (PerformanceKey key : explainCache.keySet()) {
			String rawExplainOutput = explainCache.get(key);
			List<String> structureBaseNamesUsed;
			structureBaseNamesUsed = queryPlanParser.searchForPhysicalStructureBaseNamesInRawExplainOutput(rawExplainOutput);
			
			boolean isStale = false;
			for (String structureBaseName : structureBaseNamesUsed)
				if (!deployedStructureNamesToDeployedStructures.containsKey(structureBaseName)) {
					isStale = true;
					stalePhysicalStructureBaseNames.add(structureBaseName);
					break;
				}
			if (isStale) {
				keysToRemove.add(key);
				++ staleExplainEntries;
			}
		}
		for (PerformanceKey key : keysToRemove) 
			explainCache.remove(key);
		///////
		System.err.println("We removed " + stalePerformanceEntries + " stalePerformanceEntries and " + staleExplainEntries + " staleExplainEntries: ");
		for (String structureBaseName : stalePhysicalStructureBaseNames)
			System.err.print(structureBaseName + ", ");
		System.err.println();
	}

	
	public Set<DeployedPhysicalStructure> getDeployedPhysicalStructures() {
		Set<DeployedPhysicalStructure> deployedPhysicalStructures = new HashSet<DeployedPhysicalStructure>(deployedStructureNamesToDeployedStructures.values());
		return deployedPhysicalStructures;
	}
	 
	public Set<String> getDeployedPhysicalStructureBaseNames() {
		return deployedStructureNamesToDeployedStructures.keySet();
	}
	
	public void forgetAllDeployedPhysicalStructures() {
		physicalStructureToDiskSizeInGigabytes.clear();
		physicalStructureToDeployedNames.clear();
		deployedStructureNamesToStructures.clear();
		deployedStructureNamesToDeployedStructures.clear();
	}
	
	public void emptyPerformanceCache() {
		performanceCache = new SafeMap<PerformanceKey, PerformanceValue>(); 
	}
	
	public void emptyAllDesignExecutionTraces() {
		designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>();
	}
	
	public Double getPhysicalStructureDiskSize(PhysicalStructure structure) {
		if (physicalStructureToDiskSizeInGigabytes.containsKey(structure))
			return physicalStructureToDiskSizeInGigabytes.get(structure);
		else
			return null;
	}

	public String getPhysicalStructureDeployedName(PhysicalStructure structure) {
		if (physicalStructureToDeployedNames.containsKey(structure))
			return physicalStructureToDeployedNames.get(structure);
		else
			return null;
	}

	public PhysicalStructure getPhysicalStructure(String deployedPhysicalStructureName) {
		if (deployedStructureNamesToStructures.containsKey(deployedPhysicalStructureName))
			return deployedStructureNamesToStructures.get(deployedPhysicalStructureName);
		else
			return null;	
	}

	public void cacheExecutionTrace(DesignKey key, long totalRunningTimeInSeconds, String message) throws FileNotFoundException, IOException {
		List<DesignExecutionTrace> newValue;
		if (designExecutionTrace.containsKey(key)) {
			//System.err.println("The cache already contains a DesignExecutionTrace with the same key, you shouldn't be resending this: " + key);
			newValue = designExecutionTrace.get(key);
		} else {
			newValue = new ArrayList<DesignExecutionTrace>();
		}
		
		DesignExecutionTrace trace = new DesignExecutionTrace(totalRunningTimeInSeconds, message);
		newValue.add(trace);
		
		designExecutionTrace.put(key, newValue);
		++unsyncedDesigns;
		if (unsyncedDesigns >= synchronizationFrequencyForDesigns) {
			saveTheEntireCache();
			unsyncedDesigns = 0;
		}
	}

	
	public void cacheDesign(DesignKey key, PhysicalDesign design, long totalRunningTimeInSeconds, String algorithmMessage) throws FileNotFoundException, IOException {
		if (designCache.containsKey(key))
			System.err.println("The cache already contains a Design with the same key, you shouldn't be resending this: " + key);

		cacheExecutionTrace(key, totalRunningTimeInSeconds, algorithmMessage);
		
		designCache.put(key, design);
		++unsyncedDesigns;
		if (unsyncedDesigns >= synchronizationFrequencyForDesigns) {
			saveTheEntireCache();
			unsyncedDesigns = 0;
		}
	}

	public void cacheDesignByQueriesAlgorithm(List<String> queries, String algorithmName, String algorithmParams, PhysicalDesign design, long totalRunningTimeInSeconds, String algorithmMessage) throws FileNotFoundException, IOException {
		DesignKey key = DesignKey.createDesignKeyByQueriesAlgorithm(queries, algorithmName, algorithmParams);
		cacheDesign(key, design, totalRunningTimeInSeconds, algorithmMessage);
	}

	public void cacheDesignByQueries(List<String> queries, PhysicalDesign design, long totalRunningTimeInSeconds, String algorithmMessage) throws FileNotFoundException, IOException {
		DesignKey key = DesignKey.createDesignKeyByQueries(queries);
		cacheDesign(key, design, totalRunningTimeInSeconds, algorithmMessage);
	}
	
	public void cacheDesignByWeightedQueriesAlgorithm(List<WeightedQuery> weightedQueries, String algorithmName, String algorithmParams, PhysicalDesign design, long totalRunningTimeInSeconds, String algorithmMessage) throws FileNotFoundException, IOException {
		DesignKey key = DesignKey.createDesignKeyByWeightedQueriesAlgorithms(weightedQueries, algorithmName, algorithmParams);
		cacheDesign(key, design, totalRunningTimeInSeconds, algorithmMessage);
	}

	public void cacheDesignByWeightedQueries(List<WeightedQuery> weightedQueries, PhysicalDesign design, long totalRunningTimeInSeconds, String algorithmMessage) throws FileNotFoundException, IOException {
		DesignKey key = DesignKey.createDesignKeyByWeightedQueries(weightedQueries);
		cacheDesign(key, design, totalRunningTimeInSeconds, algorithmMessage);
	}
	
	public PhysicalDesign getDesignByWeight(List<WeightedQuery> weightedQueries) {
		return getDesignByWeight(weightedQueries, null, null);
	}

	public PhysicalDesign getDesignByWeight(List<WeightedQuery> weightedQueries, String algorithmName, String algorithmParams) {
		DesignKey key = DesignKey.createDesignKeyByWeightedQueriesAlgorithms(weightedQueries, algorithmName, algorithmParams);
		if (!designCache.containsKey(key)) {
			++designsMissed;
			return null;
		} else {
			++designsHit;
			return designCache.get(key);
		}
	}
	
	public PhysicalDesign getDesign(List<String> queries) {
		return getDesign(queries, null, null);
	}
	
	public PhysicalDesign getDesignByKey(DesignKey key) {
		if (!designCache.containsKey(key)) {
			++designsMissed;
			return null;
		} else {
			++designsHit;
			return designCache.get(key);
		}
	}
	
	public PhysicalDesign getDesign(List<String> queries, String algorithmName, String algorithmParams) {
		DesignKey key = DesignKey.createDesignKeyByQueriesAlgorithm(queries, algorithmName, algorithmParams);
		if (!designCache.containsKey(key)) {
			++designsMissed;
			return null;
		} else {
			++designsHit;
			return designCache.get(key);
		}
	}

	public Map<DesignKey, PhysicalDesign> getAllDesigns() {
		return Collections.unmodifiableMap(designCache);
	}
	
	public List<DesignExecutionTrace> getDesignExecutionTrace(List<String> queries, String algorithmName, String algorithmParams) {
		DesignKey key = DesignKey.createDesignKeyByQueriesAlgorithm(queries, algorithmName, algorithmParams);
		if (!designExecutionTrace.containsKey(key)) {
			return null;
		} else {
			return designExecutionTrace.get(key);
		}
	}

	public void printAllDesignTimesToCSVFiles() throws IOException {
		PrintWriter mainFile = new PrintWriter("/tmp/allRunTimes.csv");
		PrintWriter summaryFile = new PrintWriter("/tmp/summaryRunTimes.csv");

		mainFile.write("algorithmParams\tRunTimeInSeconds\n");
		summaryFile.write("algorithmParams\tAvgRunTimeInSeconds\tNumberOfInstances\n");

		Map<String, Long> algorithmSumRuntimes = new HashMap<String, Long>();
		Map<String, Long> algorithmInstanceCounter = new HashMap<String, Long>();
		
		for (DesignKey key : designExecutionTrace.keySet()) {
			List<DesignExecutionTrace> traces = designExecutionTrace.get(key);
			DesignExecutionTrace trace = traces.get(0);
			String algorithmName = key.getAlgorithmName();
			if (algorithmName == null)
				algorithmName = "null";
			algorithmName = RobustDesigner.replaceDistibutionDistanceFromSignature(algorithmName);
			String params = key.getAlgorithmName();
			if (params == null)
				params = "null";
			params = RobustDesigner.replaceDistibutionDistanceFromSignature(params);
			long time = trace.getTotalTimeInSeconds();
			String signature = algorithmName + ":" + params;
			mainFile.write(signature + "\t" + time + "\n");
			
			if (algorithmInstanceCounter.containsKey(signature)) {
				algorithmInstanceCounter.put(signature, algorithmInstanceCounter.get(signature)+1);
			} else {
				algorithmInstanceCounter.put(signature, (long) 1);
			}
			
			if (algorithmSumRuntimes.containsKey(signature)) {
				algorithmSumRuntimes.put(signature, algorithmSumRuntimes.get(signature)+ time);
			} else {
				algorithmSumRuntimes.put(signature, time);
			}
		}
		
		
		for (String sign : algorithmSumRuntimes.keySet()) {
			double avg = algorithmSumRuntimes.get(sign) / (double) algorithmInstanceCounter.get(sign);
			summaryFile.write(sign + "\t" + avg + "\t" +  algorithmInstanceCounter.get(sign) + "\n");
		}
		
		mainFile.close();
		summaryFile.close();
		System.out.println("Files written to /tmp!");
	}

	
	public List<DesignExecutionTrace> getDesignExecutionTrace(String inputSignature, List<String> queries) {
		List<DesignExecutionTrace> trace = null;
		for (DesignKey key : designExecutionTrace.keySet()) {
			String signature = DesignAlgorithm.computeSignatureString(key.getAlgorithmName(), key.getAlgorithmParams());
			if (signature.equals(inputSignature)) { // then we have found part of the key!
				DesignKey acutalKey = DesignKey.createDesignKeyByQueriesAlgorithm(queries, key.getAlgorithmName(), key.getAlgorithmParams());
				if (designExecutionTrace.containsKey(acutalKey)) {
					trace = designExecutionTrace.get(acutalKey);
					break;
				}
			}
		}
		
		return trace;
	}

	
	public Map<DesignKey, List<DesignExecutionTrace>> getAllDesignExecutionTraces() {
		return Collections.unmodifiableMap(designExecutionTrace);
	}
	
	public void cachePerformance(String query, PhysicalDesign allowedPhysicalStructures, boolean isAccurate, PerformanceValue performanceValue) throws FileNotFoundException, IOException {
		PerformanceKey key = new PerformanceKey(query, allowedPhysicalStructures, isAccurate);
		if (performanceCache.containsKey(key))
			System.err.println("The cache already contains a performance with the same key, you shouldn't be resending this. query: " + query + "\nIsAccurate: " + isAccurate + "\nallowed structures:" + allowedPhysicalStructures);		
		performanceCache.put(key, performanceValue);
		//now insert the approximate version!
		if (isAccurate) {
			PerformanceKey approxKey = new PerformanceKey(query, allowedPhysicalStructures, false);
			PerformanceValue approxPerformanceValue = new PerformanceValue(performanceValue.getQueryPlan(), performanceValue.getAllOptimizerCosts());
			if (!performanceCache.containsKey(approxKey)) {
				performanceCache.put(approxKey, approxPerformanceValue);
			} else if (performanceCache.get(approxKey).getMeanLatency() != approxPerformanceValue.getMeanLatency()) {
				System.err.println("You previously had a key: query="+query +",\n allowedPhysicalStructures="+allowedPhysicalStructures + "\n, with old approx value=" 
						+ performanceCache.get(approxKey).getMeanLatency() + " but now we have " + approxPerformanceValue.getMeanLatency());
			}
		}
		
		//now insert the explain entry
		String rawQueryPlan = performanceValue.getQueryPlan();
		
		if (getExplain(query, allowedPhysicalStructures)==null)
			cacheExplain(query, allowedPhysicalStructures, rawQueryPlan);
		else {
			String oldCanonicalPlan = queryPlanParser.extractCanonicalQueryPlan(getExplain(query, allowedPhysicalStructures)); 
			String newCanonicalPlan = queryPlanParser.extractCanonicalQueryPlan(rawQueryPlan);
			if (!oldCanonicalPlan.equals(newCanonicalPlan))
				System.err.println("You previously had a key: query="+query +",\n allowedPhysicalStructures="+allowedPhysicalStructures + "\n, with old value="
						+ oldCanonicalPlan + "\nBut now you have a new value="+newCanonicalPlan);
		}
		
		//now insert the lower level key
		String canonicalQueryPlan = queryPlanParser.extractCanonicalQueryPlan(rawQueryPlan);
		PerformanceKey lowerKey = new PerformanceKey(query, canonicalQueryPlan, isAccurate);
		if (performanceCache.get(lowerKey)==null)
			performanceCache.put(lowerKey, performanceValue);
		else if (performanceCache.get(lowerKey).getMeanLatency() != performanceValue.getMeanLatency())
			System.err.println("Two different latencies for the same query and same query plan: " + query + "\n" + canonicalQueryPlan + "\n" + "latencies: " +  performanceCache.get(lowerKey).getMeanLatency() + " and " + performanceValue.getMeanLatency());		
	
		//now insert the approximate version of the lower level key!
		if (isAccurate) {
			PerformanceKey approxLowerKey = new PerformanceKey(query, canonicalQueryPlan, false);
			PerformanceValue approxPerformanceValue = new PerformanceValue(performanceValue.getQueryPlan(), performanceValue.getAllOptimizerCosts());
			if (!performanceCache.containsKey(approxLowerKey)) {
				performanceCache.put(approxLowerKey, approxPerformanceValue);
			} else if (performanceCache.get(approxLowerKey).getMeanLatency() != approxPerformanceValue.getMeanLatency()) {
				System.err.println("You previously had a key: query="+query +",\n canonicalQueryPlan="+canonicalQueryPlan + "\n, with old approx value=" 
						+ performanceCache.get(approxLowerKey).getMeanLatency() + " but now we have " + approxPerformanceValue.getMeanLatency());
			}
		}
		
		++unsyncedPerformanceRecords;
		if (unsyncedPerformanceRecords >= synchronizationFrequencyForPerformanceRecords) {
			saveTheEntireCache();
			unsyncedPerformanceRecords = 0;
		}
	}
	
	public PerformanceValue getPerformance(String query, PhysicalDesign allowedPhysicalStructures, boolean isAccurate) {
		PerformanceKey key = new PerformanceKey(query, allowedPhysicalStructures, isAccurate);
		if (!performanceCache.containsKey(key)) {
			++verbatimPerformanceMisses;
			return null;
		} else {
			++verbatimPerformanceHits;
			return performanceCache.get(key);
		}
	}

	public PerformanceValue getPerformance(String query, String queryPlan, boolean isAccurate) {
		String canonicalQueryPlan = queryPlanParser.extractCanonicalQueryPlan(queryPlan);
		PerformanceKey key = new PerformanceKey(query, canonicalQueryPlan, isAccurate);
		if (!performanceCache.containsKey(key)){
			++planBasedPerformanceMisses;
			return null;
		} else {
			++planBasedPerformanceHits;
			return performanceCache.get(key);
		}
	}

	public boolean invalidatePerformance(String query, PhysicalDesign allowedPhysicalStructures) throws FileNotFoundException, IOException {
		PerformanceKey key1 = new PerformanceKey(query, allowedPhysicalStructures, true);
		PerformanceKey key2 = new PerformanceKey(query, allowedPhysicalStructures, false);
		boolean contained = false;
		
		if (performanceCache.containsKey(key1)) {
			performanceCache.remove(key1);
			contained = true;
		}
		if (performanceCache.containsKey(key2)) {
			performanceCache.remove(key2);
			contained = true;
		}
		
		if (contained) {
			saveTheEntireCache();
			return true;
		} else {
			return false;
		}
	}
	
	public void cacheExplain(String query, PhysicalDesign allowedPhysicalStructures, String rawExplainOutput) throws FileNotFoundException, IOException {
		PerformanceKey key = new PerformanceKey(query, allowedPhysicalStructures, false);
		if (explainCache.containsKey(key))
			System.err.println("The cache already contains a explain object with the same key, you shouldn't be resending this. query: " + query + "\nAllowedStructs:\n" + allowedPhysicalStructures);		
		explainCache.put(key, rawExplainOutput);
		
		++unsyncedPerformanceRecords;
		if (unsyncedPerformanceRecords >= synchronizationFrequencyForPerformanceRecords) {
			saveTheEntireCache();
			unsyncedPerformanceRecords = 0;
		}
	}
	
	public String getExplain(String query, PhysicalDesign allowedPhysicalStructures) {
		PerformanceKey key = new PerformanceKey(query, allowedPhysicalStructures, false);
		
		if (explainCache==null)
			explainCache = new SafeMap<ExperimentCache.PerformanceKey, String>();
		
		if (!explainCache.containsKey(key)) {
			++explainCommandMisses;
			return null;
		} else {
			++explainCommandHits;
			return explainCache.get(key);
		}
	}


	private void consolidateCache() {
		Map<PhysicalStructure, PhysicalStructure> allStructs = new HashMap<PhysicalStructure, PhysicalStructure>();
		
		//private SafeMap<DesignKey, PhysicalDesign> designCache = new SafeMap<DesignKey, PhysicalDesign>();
		SafeMap<DesignKey, PhysicalDesign> newDesignCache = new SafeMap<DesignKey, PhysicalDesign>();
		for (DesignKey key : designCache.keySet()) {
			List<PhysicalStructure> newList = new ArrayList<PhysicalStructure>();
			for (PhysicalStructure ps : designCache.get(key).getPhysicalStructuresAsList()) {
				if (!allStructs.containsKey(ps))
					allStructs.put(ps, ps);
				newList.add(allStructs.get(ps));
			}
			newDesignCache.put(key, new PhysicalDesign(newList));
		}
		designCache = newDesignCache;
		
		//private SafeMap<DesignKey, List<DesignExecutionTrace>> designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>(); 
		
		//private SafeMap<PerformanceKey, PerformanceValue> performanceCache = new SafeMap<PerformanceKey, PerformanceValue>();
		SafeMap<PerformanceKey, PerformanceValue> newPerformanceCache = new SafeMap<ExperimentCache.PerformanceKey, PerformanceValue>();
		List<PerformanceKey> keyList = new ArrayList<PerformanceKey>(performanceCache.keySet());
		while (!keyList.isEmpty()) {
			PerformanceKey key = keyList.get(0);
			PerformanceKey newKey;
			if (key.canonicalQueryPlan == null) {
				List<PhysicalStructure> newList = new ArrayList<PhysicalStructure>();
				for (PhysicalStructure ps: key.allowedPhysicalStructures.getPhysicalStructuresAsList()) {
					if (!allStructs.containsKey(ps))
						allStructs.put(ps, ps);
					newList.add(allStructs.get(ps));
				}
				
				newKey = new PerformanceKey(key.querySql, new PhysicalDesign(newList), key.isAccurate);
			} else {
				newKey = new PerformanceKey(key.querySql, key.canonicalQueryPlan, key.isAccurate);
			}
			newPerformanceCache.put(newKey, performanceCache.get(key));
			keyList.remove(0);
			performanceCache.remove(key);
		}
		performanceCache = newPerformanceCache;
		
		//private SafeMap<PerformanceKey, String> explainCache = new SafeMap<PerformanceKey, String>();
		SafeMap<PerformanceKey, String> newExplainCache = new SafeMap<PerformanceKey, String>();
		for (PerformanceKey key : explainCache.keySet()) {
			PerformanceKey newKey;
			if (key.canonicalQueryPlan == null) {
				List<PhysicalStructure> newList = new ArrayList<PhysicalStructure>();
				for (PhysicalStructure ps: key.allowedPhysicalStructures.getPhysicalStructuresAsList()) {
					if (!allStructs.containsKey(ps))
						allStructs.put(ps, ps);
					newList.add(allStructs.get(ps));
				}
				
				newKey = new PerformanceKey(key.querySql, new PhysicalDesign(newList), key.isAccurate);
			} else {
				newKey = new PerformanceKey(key.querySql, key.canonicalQueryPlan, key.isAccurate);
			}
			newExplainCache.put(newKey, explainCache.get(key));
		}
		explainCache = newExplainCache;
		
		//private SafeMap<PhysicalStructure, Double> physicalStructureToDiskSizeInGigabytes = new SafeMap<PhysicalStructure, Double>();
		SafeMap<PhysicalStructure, Double> newPhysicalStructureToDiskSizeInGigabytes = new SafeMap<PhysicalStructure, Double>();
		for (PhysicalStructure ps : physicalStructureToDiskSizeInGigabytes.keySet()) {
			if (!allStructs.containsKey(ps))
				allStructs.put(ps, ps);
			newPhysicalStructureToDiskSizeInGigabytes.put(allStructs.get(ps), physicalStructureToDiskSizeInGigabytes.get(ps));
		}
		physicalStructureToDiskSizeInGigabytes = newPhysicalStructureToDiskSizeInGigabytes;
		
		//private SafeMap<PhysicalStructure, String> physicalStructureToDeployedNames = new SafeMap<PhysicalStructure, String>();
		SafeMap<PhysicalStructure, String> newPhysicalStructureToDeployedNames = new SafeMap<PhysicalStructure, String>();
		for (PhysicalStructure ps : physicalStructureToDeployedNames.keySet()) {
			if (!allStructs.containsKey(ps))
				allStructs.put(ps, ps);
			newPhysicalStructureToDeployedNames.put(allStructs.get(ps), physicalStructureToDeployedNames.get(ps));
		}
		physicalStructureToDeployedNames = newPhysicalStructureToDeployedNames;
		
		//private SafeMap<String, PhysicalStructure> deployedStructureNamesToStructures = new SafeMap<String, PhysicalStructure>();
		SafeMap<String, PhysicalStructure> newDeployedStructureNamesToStructures = new SafeMap<String, PhysicalStructure>();
		for (String key : deployedStructureNamesToStructures.keySet()) {
			PhysicalStructure ps = deployedStructureNamesToStructures.get(key);
			if (!allStructs.containsKey(ps))
				allStructs.put(ps, ps);
			newDeployedStructureNamesToStructures.put(key, allStructs.get(ps));
		}
		deployedStructureNamesToStructures = newDeployedStructureNamesToStructures;
		
		//private SafeMap<String, DeployedPhysicalStructure> deployedStructureNamesToDeployedStructures = new SafeMap<String, DeployedPhysicalStructure>();		
	}

	
	public void saveTheEntireCache(String filename) throws FileNotFoundException, IOException {
		Timer t = new Timer();
		
		// consolidateCache();
		
		// Existing file with this name
	    File file1 = new File(filename);
	    File oldFile = new File(filename + ".prevVersion");
	    
	    if (file1.exists())
	    	if (!file1.renameTo(oldFile)) {
	    		// DY: renameTo() is known to be problematic in Windows, so if it fails, let's try Files.move in Java 7
	    		if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
	    			String targetFilename = oldFile.getName();
	    			Path sourcePath = file1.toPath();
	    			Files.move(sourcePath, sourcePath.resolveSibling(targetFilename), StandardCopyOption.REPLACE_EXISTING);
	    		}
	    		else {
	    			throw new IOException("We could not rename the existing file with name : " + filename);
	    		}
	    	}
	
	    // now we have already backed up the existing file
	    // writeToNormalFile(this, newFileName);
 		writeToRandomFile(this, filename); // slightly faster!
	 	
 		secondsSpentWritingToFile += t.lapSeconds();
 		++numberOfWriteOperationsToFile;
	}

	public void saveTheEntireCache() throws FileNotFoundException, IOException {
		saveTheEntireCache(newFileName); 
	}
	
	private static void writeToNormalFile(ExperimentCache experimentCache, String filename) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		oos.writeObject(experimentCache);
		oos.close();
	}
	
	private static void writeToFile(ExperimentCache experimentCache, String filename) throws FileNotFoundException, IOException {
		writeToRandomFile(experimentCache, filename);
		//writeToNormalFile(experimentCache, filename);
	}
	
	private static void writeToRandomFile(ExperimentCache experimentCache, String filename) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = null;
		RandomAccessFile raf = new RandomAccessFile(filename, "rw");
		FileOutputStream fos = new FileOutputStream(raf.getFD());
		oos = new ObjectOutputStream(new BufferedOutputStream(fos));
		oos.writeObject(experimentCache);
		oos.close();
	}
		
	public String reportStatistics() {
		StringBuilder stats = new StringBuilder();
		stats.append("Cache contains " + designCache.size()+ " designs, " + designExecutionTrace.size() + " execution traces, " +physicalStructureToDiskSizeInGigabytes.size() + " struct sizes, " + physicalStructureToDeployedNames.size() + " struct names, " 
				+ deployedStructureNamesToStructures.size() + " struct structs, and " + performanceCache.size() + " query latencies.\n");
		stats.append("designsHit="+designsHit+", designsMissed="+designsMissed +" (hit ratio=" + 100*designsHit/(designsHit+designsMissed+1)+"%)\n");
		//because performanceHits get counted twice! They get called once to see if they exist and then they get called again to retrieve!
		stats.append("verbatimPerformanceHits="+verbatimPerformanceHits/2+", verbatimPerformanceMisses="+verbatimPerformanceMisses +" (hit ratio=" + 100*0.5*verbatimPerformanceHits/(0.5*verbatimPerformanceHits+verbatimPerformanceMisses+1)+"%)\n");
		stats.append("planBasedPerformanceHits="+planBasedPerformanceHits/2+", planBasedPerformanceMisses="+planBasedPerformanceMisses +" (hit ratio=" + 100*0.5*planBasedPerformanceHits/(0.5*planBasedPerformanceHits+planBasedPerformanceMisses+1)+"%)\n");
		stats.append("explainCommandHits="+explainCommandHits/2+", explainCommandMisses="+explainCommandMisses +" (hit ratio=" + 100*0.5*explainCommandHits/(0.5*explainCommandHits+explainCommandMisses+1)+"%)\n");
		stats.append("minsSpentLoadingFromFile=" + secondsSpentLoadingFromFile/60 + ", minsSpentWritingToFile="+secondsSpentWritingToFile/60 + 
					", numberOfWriteOperationsToFile=" + numberOfWriteOperationsToFile + "\n");

		return stats.toString();
	}
	
	public String getOriginal_filename() {
		return original_filename;
	}

	public void setOriginal_filename(String original_filename) {
		this.original_filename = original_filename;
	}

	public void repairParams(String patternStart, String patternEnd, String newString) {
		SafeMap<DesignKey, PhysicalDesign> newDesignCache = new SafeMap<DesignKey, PhysicalDesign>();

		for (DesignKey key : designCache.keySet()) {
			PhysicalDesign value = designCache.get(key);
			String algorithmParams = key.getAlgorithmParams();
			if (algorithmParams != null) {
				int start = algorithmParams.indexOf(patternStart);
				if (start!=-1) {
					int end = algorithmParams.substring(start).indexOf(patternEnd);
					if (end!=-1) {
						String toReplace = algorithmParams.substring(start, start+end+1);
						String newParams = algorithmParams.replaceFirst(toReplace, newString);
						DesignKey newKey = DesignKey.createDesignKeyByWeightedQueriesAlgorithms(
								new ArrayList<WeightedQuery>(key.getWeightedQueries()), key.getAlgorithmName(), newParams);
						newDesignCache.put(newKey, value);
						continue;
					}
				}
			}
			newDesignCache.put(key, value);
		}
		designCache = newDesignCache;
	}
	
	public static ExperimentCache loadCacheFromFile(String filename) throws Exception {
		return loadCacheFromFile(filename, GlobalConfigurations.SynchronizationFrequencyForPerformanceRecords, GlobalConfigurations.SynchronizationFrequencyForDesigns, false);
	}

	public static ExperimentCache loadCacheFromFileForMerging(String filename) throws Exception {
		return loadCacheFromFile(filename, GlobalConfigurations.SynchronizationFrequencyForPerformanceRecords, GlobalConfigurations.SynchronizationFrequencyForDesigns, true);
	}

	public static ExperimentCache loadCacheFromFile(String filename, int synchronizationFrequencyForPerformanceRecords, int synchronizationFrequencyForDesigns) throws Exception {
		return loadCacheFromFile(filename, synchronizationFrequencyForPerformanceRecords, synchronizationFrequencyForDesigns, false);
	}
	
	public static ExperimentCache loadCacheFromFile(String filename, int synchronizationFrequencyForPerformanceRecords, int synchronizationFrequencyForDesigns, boolean forMergingOnly) throws Exception {
		ExperimentCache cache = null;
		ObjectInputStream in;
		try {
			Timer t = new Timer();
			in = new ObjectInputStream(new FileInputStream(new File(filename)));
			try {
				cache = (ExperimentCache) in.readObject();
				cache.setOriginal_filename(filename);
				cache.synchronizationFrequencyForPerformanceRecords = synchronizationFrequencyForPerformanceRecords;
				cache.synchronizationFrequencyForDesigns = synchronizationFrequencyForDesigns;
				String dateStr = new SimpleDateFormat("yyyy-MM-dd_HHmm").format(new Date());
				cache.newFileName = filename + "." + dateStr;
				cache.designsHit = 0;
				cache.designsMissed = 0;
				cache.verbatimPerformanceHits = 0;
				cache.verbatimPerformanceMisses = 0;
				cache.planBasedPerformanceHits = 0;
				cache.planBasedPerformanceMisses = 0;
				cache.explainCommandHits = 0;
				cache.explainCommandMisses = 0;
				cache.secondsSpentLoadingFromFile = (long) t.lapSeconds();
				cache.secondsSpentWritingToFile = 0;
				cache.numberOfWriteOperationsToFile = 0;
				if (cache.performanceCache == null)
					cache.performanceCache = new SafeMap<ExperimentCache.PerformanceKey, PerformanceValue>();
				if (cache.designExecutionTrace == null)
					cache.designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>();
				if (cache.queryPlanParser == null) {
					if (System.getProperty("os.name").toLowerCase().startsWith("windows"))
						cache.queryPlanParser = new MicrosoftQueryPlanParser();
					else
						cache.queryPlanParser = new VerticaQueryPlanParser();
					//TODO: This is a dirty check and can cause a lot of bugs!
				}
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			in.close();
		} catch (IOException e) {
			//e.printStackTrace();
			System.out.println("Our effort to load cache from file failed : " + e.getLocalizedMessage());
		}
		return cache;		
	}

	public static ExperimentCache transformOldCacheFormatIntoNewOne(String oldFormatCacheFileName, String newFormatCacheFileName) throws Exception {
		System.out.println("Please uncomment this function aftering adding the old version as edu.mit package!");
		return null;
		/*
		edu.mit.robustopt.metering.ExperimentCache oldCache = null;
		ExperimentCache newCache = null;
		ObjectInputStream in;
		try {
			Timer t = new Timer();
			in = new ObjectInputStream(new FileInputStream(new File(oldFormatCacheFileName)));
			try {
				oldCache = (edu.mit.robustopt.metering.ExperimentCache) in.readObject();
				newCache = new ExperimentCache(newFormatCacheFileName, oldCache.getSynchronizationFrequencyForPerformanceRecords(), oldCache.getSynchronizationFrequencyForDesigns(), 
						oldCache.getSynchronizationFrequencyForDeployedProjections());
					
				
				// we first create a giant bank of all VerticaProjectionStructure objects so that we do not creates duplicates
				// and use their references whenever there is a need for them!
				Map<PhysicalStructure, PhysicalStructure> allStructures = new HashMap<PhysicalStructure, PhysicalStructure>();
				
				// the following are the main pieces of information that need to be transferred!
				
				//private SafeMap<DesignKey, List<VerticaProjectionStructure>> designCache = new SafeMap<DesignKey, List<VerticaProjectionStructure>>();
				newCache.designCache = new SafeMap<DesignKey, PhysicalDesign>();
				Map<edu.mit.robustopt.metering.DesignKey, List<edu.mit.robustopt.preprocessing.VerticaProjectionStructure>> oldDesignCache = oldCache.getAllDesigns();
				for (edu.mit.robustopt.metering.DesignKey oldKey : oldDesignCache.keySet()) {
					List<edu.mit.robustopt.preprocessing.VerticaProjectionStructure> oldDesign = oldDesignCache.get(oldKey);
					Set<edu.mit.robustopt.util.WeightedQuery> oldWeightedQueries = oldKey.getWeightedQueries();
					List<WeightedQuery> newWeightedQueries = new ArrayList<WeightedQuery>();
					for (edu.mit.robustopt.util.WeightedQuery w : oldWeightedQueries) 
						newWeightedQueries.add(new WeightedQuery(w.query, w.weight));
					
					String oldName = oldKey.getAlgorithmName();
					if (oldName!=null && oldName.contains(".mit.")) {
						System.err.print("We saw: " + oldName + " fixed to ");
						oldName = oldName.replaceAll(".mit.", ".umich.");
						System.err.println(oldName);
					}
					
					DesignKey newKey = DesignKey.createDesignKeyByWeightedQueriesAlgorithms(newWeightedQueries, oldName, oldKey.getAlgorithmParams());
					Set<PhysicalStructure> physicalStructures = new HashSet<PhysicalStructure>();
					for (edu.mit.robustopt.preprocessing.VerticaProjectionStructure p : oldDesign) {
						edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = p.getProjection_anchor_table();
						NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
						VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , p.getProjection_columns(), p.getProjection_column_datatypes(), p.getProjection_column_encodings(), p.getProjection_sort_order()); 
						if (allStructures.containsKey(newP))
							newP = (VerticaProjectionStructure) allStructures.get(newP);
						else
							allStructures.put(newP, newP);
						physicalStructures.add(newP);
					}
					PhysicalDesign newDesign = new PhysicalDesign(physicalStructures);
					if (newCache.designCache.containsKey(newKey))
						throw new Exception("Duplicate key!" + newKey + ", " + newCache.designCache.get(newKey));
					newCache.designCache.put(newKey, newDesign);
				}
				
				
				
				//private SafeMap<DesignKey, List<DesignExecutionTrace>> designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>(); 
				newCache.designExecutionTrace = new SafeMap<DesignKey, List<DesignExecutionTrace>>();
				Map<edu.mit.robustopt.metering.DesignKey, List<edu.mit.robustopt.metering.DesignExecutionTrace>> oldDesignExecutionTrace = oldCache.getAllDesignExecutionTraces();
				for (edu.mit.robustopt.metering.DesignKey oldKey : oldDesignExecutionTrace.keySet()) {
					List<edu.mit.robustopt.metering.DesignExecutionTrace> oldExecTrace = oldDesignExecutionTrace.get(oldKey);
					
					Set<edu.mit.robustopt.util.WeightedQuery> oldWeightedQueries = oldKey.getWeightedQueries();
					List<WeightedQuery> newWeightedQueries = new ArrayList<WeightedQuery>();
					for (edu.mit.robustopt.util.WeightedQuery w : oldWeightedQueries) 
						newWeightedQueries.add(new WeightedQuery(w.query, w.weight));
					
					String oldName = oldKey.getAlgorithmName();
					if (oldName!=null && oldName.contains(".mit.")) {
						System.err.print("We saw: " + oldName + " fixed to ");
						oldName = oldName.replaceAll(".mit.", ".umich.");
						System.err.println(oldName);
					}
					
					DesignKey newKey = DesignKey.createDesignKeyByWeightedQueriesAlgorithms(newWeightedQueries, oldName, oldKey.getAlgorithmParams());
					List<DesignExecutionTrace> newExecTrace = new ArrayList<DesignExecutionTrace>(); 
					for (edu.mit.robustopt.metering.DesignExecutionTrace oldT : oldExecTrace)
						newExecTrace.add(new DesignExecutionTrace(oldT.getDate(), oldT.getTotalTimeInSeconds(), oldT.getMessage()));
					if (newCache.designExecutionTrace.containsKey(newKey))
						throw new Exception("Duplicate key!" + newKey + ", " + newCache.designExecutionTrace.get(newKey)); 
					newCache.designExecutionTrace.put(newKey, newExecTrace);
				}

				//private SafeMap<PerformanceKey, PerformanceValue> performanceCache = new SafeMap<PerformanceKey, PerformanceValue>();
				newCache.performanceCache = new SafeMap<ExperimentCache.PerformanceKey, PerformanceValue>();

				edu.mit.robustopt.util.SafeMap<edu.mit.robustopt.metering.ExperimentCache.PerformanceKey, edu.mit.robustopt.metering.PerformanceValue> oldPerformanceCache = oldCache.getPerformanceCache();
				for (edu.mit.robustopt.metering.ExperimentCache.PerformanceKey oldKey : oldPerformanceCache.keySet()) {
					edu.mit.robustopt.metering.PerformanceValue oldPerfValue = oldPerformanceCache.get(oldKey);
					PerformanceKey newKey;
					if (oldKey.getAllowedProjections() == null) {
						newKey = newCache.new PerformanceKey(oldKey.getQuerySql(), oldKey.getCanonicalQueryPlan(), oldKey.isAccurate());
					} else {
						List<edu.mit.robustopt.preprocessing.VerticaProjectionStructure> oldProjs = oldKey.getAllowedProjections();
						Set<PhysicalStructure> newStructs = new HashSet<PhysicalStructure>();
						for (edu.mit.robustopt.preprocessing.VerticaProjectionStructure p : oldProjs) {							
							edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = p.getProjection_anchor_table();
							NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
							VerticaProjectionStructure newP = 
									new VerticaProjectionStructure(newAnchorTable , p.getProjection_columns(), p.getProjection_column_datatypes(), p.getProjection_column_encodings(), p.getProjection_sort_order()); 
							if (allStructures.containsKey(newP))
								newP = (VerticaProjectionStructure) allStructures.get(newP);
							else
								allStructures.put(newP, newP);
							newStructs.add(newP);

						}
						newKey = newCache.new PerformanceKey(oldKey.getQuerySql(), new PhysicalDesign(newStructs), oldKey.isAccurate());
					}
					PerformanceValue newPerfValue = new PerformanceValue(oldPerfValue.getQueryPlan(), oldPerfValue.getAllActualLatencies(), oldPerfValue.getAllOptimizerCosts());
					newCache.performanceCache.put(newKey, newPerfValue);
				}
				
				//private SafeMap<PerformanceKey, String> explainCache = new SafeMap<PerformanceKey, String>();
				newCache.explainCache = new SafeMap<ExperimentCache.PerformanceKey, String>();
				edu.mit.robustopt.util.SafeMap<edu.mit.robustopt.metering.ExperimentCache.PerformanceKey, String> oldExplainCache = oldCache.getExplainCache();
				for (edu.mit.robustopt.metering.ExperimentCache.PerformanceKey oldKey : oldExplainCache.keySet()) {
					PerformanceKey newKey;
					if (oldKey.getAllowedProjections() == null) {
						newKey = newCache.new PerformanceKey(oldKey.getQuerySql(), oldKey.getCanonicalQueryPlan(), oldKey.isAccurate());
					} else {
						List<edu.mit.robustopt.preprocessing.VerticaProjectionStructure> oldProjs = oldKey.getAllowedProjections();
						List<PhysicalStructure> newStructs = new ArrayList<PhysicalStructure>();
						for (edu.mit.robustopt.preprocessing.VerticaProjectionStructure p : oldProjs) {
							edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = p.getProjection_anchor_table();
							NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
							VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , p.getProjection_columns(), p.getProjection_column_datatypes(), p.getProjection_column_encodings(), p.getProjection_sort_order()); 
							if (allStructures.containsKey(newP))
								newP = (VerticaProjectionStructure) allStructures.get(newP);
							else
								allStructures.put(newP, newP);
							
							newStructs.add(newP);
						}
						newKey = newCache.new PerformanceKey(oldKey.getQuerySql(), new PhysicalDesign(newStructs), oldKey.isAccurate());
					}
					newCache.explainCache.put(newKey, oldExplainCache.get(oldKey));
				}
				

				//private SafeMap<VerticaProjectionStructure, Double> projectionStructureToDiskSize = new SafeMap<VerticaProjectionStructure, Double>();
				newCache.physicalStructureToDiskSizeInGigabytes = new SafeMap<PhysicalStructure, Double>();
				edu.mit.robustopt.util.SafeMap<edu.mit.robustopt.preprocessing.VerticaProjectionStructure, Double> oldDiskSize = oldCache.getProjectionStructureToDiskSize();
				for (edu.mit.robustopt.preprocessing.VerticaProjectionStructure oldKey : oldDiskSize.keySet()) {
					edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = oldKey.getProjection_anchor_table();
					NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
					VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , oldKey.getProjection_columns(), oldKey.getProjection_column_datatypes(), oldKey.getProjection_column_encodings(), oldKey.getProjection_sort_order()); 
					if (allStructures.containsKey(newP))
						newP = (VerticaProjectionStructure) allStructures.get(newP);
					else
						allStructures.put(newP, newP);
					
					Double diskSizeInGigabytes = oldDiskSize.get(oldKey);
					newP.setDiskSizeInGigabytes(diskSizeInGigabytes);
					
					newCache.physicalStructureToDiskSizeInGigabytes.put(newP, diskSizeInGigabytes);
				}
				
				//private SafeMap<VerticaProjectionStructure, String> projectionStructureToDeployedNames = new SafeMap<VerticaProjectionStructure, String>();
				newCache.physicalStructureToDeployedNames = new SafeMap<PhysicalStructure, String>();
				edu.mit.robustopt.util.SafeMap<edu.mit.robustopt.preprocessing.VerticaProjectionStructure, String> oldDeployedNames = oldCache.getProjectionStructureToDeployedNames();
				for (edu.mit.robustopt.preprocessing.VerticaProjectionStructure oldKey : oldDeployedNames.keySet()) {
					edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = oldKey.getProjection_anchor_table();
					NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
					VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , oldKey.getProjection_columns(), oldKey.getProjection_column_datatypes(), oldKey.getProjection_column_encodings(), oldKey.getProjection_sort_order()); 
					if (allStructures.containsKey(newP))
						newP = (VerticaProjectionStructure) allStructures.get(newP);
					else
						allStructures.put(newP, newP);
					newCache.physicalStructureToDeployedNames.put(newP, oldDeployedNames.get(oldKey));
				}
				
				//private SafeMap<String, VerticaProjectionStructure> deployedProjectionNamesToStructures = new SafeMap<String, VerticaProjectionStructure>();
				newCache.deployedStructureNamesToStructures = new SafeMap<String, PhysicalStructure>();
				edu.mit.robustopt.util.SafeMap<String, edu.mit.robustopt.preprocessing.VerticaProjectionStructure> oldDeployed = oldCache.getDeployedProjectionNamesToStructures();
				for (String key : oldDeployed.keySet()) {
					edu.mit.robustopt.preprocessing.VerticaProjectionStructure oldP = oldDeployed.get(key);
					edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = oldP.getProjection_anchor_table();
					NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
					VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , oldP.getProjection_columns(), oldP.getProjection_column_datatypes(), oldP.getProjection_column_encodings(), oldP.getProjection_sort_order()); 
					if (allStructures.containsKey(newP))
						newP = (VerticaProjectionStructure) allStructures.get(newP);
					else
						allStructures.put(newP, newP);
					newCache.deployedStructureNamesToStructures.put(key, newP);
				}
				
				//private SafeMap<String, VerticaDeployedProjection> deployedProjectionNamesToDeployedProjections = new SafeMap<String, VerticaDeployedProjection>();
				newCache.deployedStructureNamesToDeployedStructures = new SafeMap<String, DeployedPhysicalStructure>();
				edu.mit.robustopt.util.SafeMap<String, edu.mit.robustopt.preprocessing.VerticaDeployedProjection> oldDepT = oldCache.getDeployedProjectionNamesToDeployedProjections();
				for (String key : oldDepT.keySet()) {
					edu.mit.robustopt.preprocessing.VerticaDeployedProjection oldP = oldDepT.get(key);
					edu.mit.robustopt.preprocessing.VerticaProjectionStructure oldStruct = oldP.getProjection_structure();
					edu.mit.robustopt.util.NamedIdentifier oldAnchorTable = oldStruct.getProjection_anchor_table();
					NamedIdentifier newAnchorTable = new NamedIdentifier(oldAnchorTable.first, oldAnchorTable.second);
					VerticaProjectionStructure newP = new VerticaProjectionStructure(newAnchorTable , oldStruct.getProjection_columns(), oldStruct.getProjection_column_datatypes(), oldStruct.getProjection_column_encodings(), oldStruct.getProjection_sort_order()); 
					if (allStructures.containsKey(newP))
						newP = (VerticaProjectionStructure) allStructures.get(newP);
					else
						allStructures.put(newP, newP);
					VerticaDeployedProjection newDeployed = new VerticaDeployedProjection(oldP.getProjection_schema(), oldP.getProjection_name(), oldP.getProjection_basename(), newP);					
					newCache.deployedStructureNamesToDeployedStructures.put(key, newDeployed);
				}

				System.out.println("In this process, we saw " + allStructures.size() + " unique physical structures!");

				///
				newCache.setOriginal_filename(newFormatCacheFileName);
				newCache.synchronizationFrequencyForPerformanceRecords = oldCache.synchronizationFrequencyForPerformanceRecords;
				newCache.synchronizationFrequencyForDesigns = oldCache.synchronizationFrequencyForDesigns;
				String dateStr = new SimpleDateFormat("yyyy-MM-dd--HH:mm").format(new Date());
				newCache.newFileName = newFormatCacheFileName + "." + dateStr;
				newCache.designsHit = 0;
				newCache.designsMissed = 0;
				newCache.verbatimPerformanceHits = 0;
				newCache.verbatimPerformanceMisses = 0;
				newCache.planBasedPerformanceHits = 0;
				newCache.planBasedPerformanceMisses = 0;
				newCache.explainCommandHits = 0;
				newCache.explainCommandMisses = 0;
				newCache.secondsSpentLoadingFromFile = (long) t.lapSeconds();
				newCache.secondsSpentWritingToFile = 0;
				newCache.numberOfWriteOperationsToFile = 0;
				
				newCache.saveTheEntireCache();
				System.out.println("old stats: \n" + oldCache.reportStatistics());
				System.out.println("new stats: \n" + newCache.reportStatistics());
				
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			in.close();
		} catch (IOException e) {
			//e.printStackTrace();
			System.out.println("Our effort to load cache from file failed : " + e.getLocalizedMessage());
		}
		return newCache;		
		*/
	}

	
	public class PerformanceKey implements Serializable {
		private static final long serialVersionUID = -1667770964868435358L;
		private String querySql;
		private PhysicalDesign allowedPhysicalStructures;
		private String canonicalQueryPlan;
		private boolean isAccurate;
		
		public PerformanceKey(String querySql, PhysicalDesign whichStructuresToInclude, boolean isAccurate) {
			this.querySql = querySql;
			this.allowedPhysicalStructures = whichStructuresToInclude;
			this.canonicalQueryPlan = null;
			this.isAccurate = isAccurate;
		}
		
		public PerformanceKey(String querySql, String canonicalQueryPlan, boolean isAccurate) {
			this.querySql = querySql;
			this.allowedPhysicalStructures = null;
			this.canonicalQueryPlan = canonicalQueryPlan;
			this.isAccurate = isAccurate;
		}

		private ExperimentCache getOuterType() {
			return ExperimentCache.this;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime
					* result
					+ ((allowedPhysicalStructures == null) ? 0 : allowedPhysicalStructures
							.hashCode());
			result = prime
					* result
					+ ((canonicalQueryPlan == null) ? 0 : canonicalQueryPlan
							.hashCode());
			result = prime * result + (isAccurate ? 1231 : 1237);
			result = prime * result
					+ ((querySql == null) ? 0 : querySql.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PerformanceKey other = (PerformanceKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (allowedPhysicalStructures == null) {
				if (other.allowedPhysicalStructures != null)
					return false;
			} else if (!allowedPhysicalStructures.equals(other.allowedPhysicalStructures))
				return false;
			if (canonicalQueryPlan == null) {
				if (other.canonicalQueryPlan != null)
					return false;
			} else if (!canonicalQueryPlan.equals(other.canonicalQueryPlan))
				return false;
			if (isAccurate != other.isAccurate)
				return false;
			if (querySql == null) {
				if (other.querySql != null)
					return false;
			} else if (!querySql.equals(other.querySql))
				return false;
			return true;
		}
		
	}
	
	public void removeDesignByAlgorithmName(String algorithmName) {
		Set<DesignKey> keysToRemove = new HashSet<DesignKey>();
		for (DesignKey key : designCache.keySet()) {
			if (algorithmName == null) {
				if (key.algorithmName == null)
					keysToRemove.add(key);					
			} else if (algorithmName.equals(key.algorithmName))
				keysToRemove.add(key);
		}
		for (DesignKey key:keysToRemove)
			designCache.remove(key);
	}

	public void renameAlgorithmNames(String oldAlgorithmName, String newAlgorithmName) {
		Set<DesignKey> keysToChange = new HashSet<DesignKey>();
		for (DesignKey key : designCache.keySet()) {
			if (oldAlgorithmName == null) {
				if (key.algorithmName == null)
					keysToChange.add(key);					
			} else if (oldAlgorithmName.equals(key.algorithmName))
				keysToChange.add(key);
		}
		for (DesignKey key:keysToChange) {
			key.algorithmName = newAlgorithmName;
		}
		System.out.println("We renamed " + keysToChange.size() + " keys from " + oldAlgorithmName + " to " + newAlgorithmName);
	}


	public Set<DesignKey> findDesignsByTheirSuperClass(Class class1) throws ClassNotFoundException {
		Set<DesignKey> keysFound = new HashSet<DesignKey>();
		for (DesignKey key : designCache.keySet()) {
			String algorithmName = key.algorithmName;
			if (algorithmName == null)
				continue;
			try {
				Class class2 = Class.forName("edu.umich.robustopt.algorithms." + algorithmName);
				if (class1.isAssignableFrom(class2)) {
					System.err.println("Attention: we identified a design by algorithm " + algorithmName + " which is a sub-class of " + class1);
					keysFound.add(key);
				}
			} catch (ClassNotFoundException e) {
				System.err.println("Error message: " + e.getMessage());
				//keysToRemove.add(key);
			}
		}
		
		return keysFound;
	}

	public void removeDesignsByTheirSuperClass(Class class1) throws ClassNotFoundException {
		Set<DesignKey> keysToRemove = findDesignsByTheirSuperClass(class1);
		
		for (DesignKey key:keysToRemove)
			designCache.remove(key);
	}



	public String getNewFileName() {
		return newFileName;
	}


	public void setNewFileName(String newFileName) {
		this.newFileName = newFileName;
	}
	
	public void mergeDesigns(ExperimentCache other) {
		for (DesignKey key : other.designCache.keySet())
			if (!this.designCache.containsKey(key))
				this.designCache.put(key, other.designCache.get(key));
			else if (this.designCache.get(key).equals(other.designCache.get(key)))
				System.err.println("Design is non-deterministic!");
	}
	
	private static void writeToRandomFile(Object object, String filename) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = null;
		RandomAccessFile raf = new RandomAccessFile(filename, "rw");
		FileOutputStream fos = new FileOutputStream(raf.getFD());
		oos = new ObjectOutputStream(new BufferedOutputStream(fos));
		oos.writeObject(object);
		oos.close();
	}

	private static void writeToNormalFile(Object object, String filename) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
		oos.writeObject(object);
		oos.close();
	}

	public QueryPlanParser getQueryPlanParser () {
		return queryPlanParser;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("You need to provide one of these two commands:");
			System.err.println("Usage: ExperimentCache merge cacheFileToExtractDesignsFrom outputMergeFile [cacheFileToAddDesignsTo] ");
			System.err.println("Usage: ExperimentCache transform cacheFileInOldFormat filenameForNewFormatCache ");
			System.err.println("Usage: ExperimentCache renameAlgorithm oldCacheFile newCacheFile ");
			return;
		} else if (args[0].equals("merge")) {
			if (args.length != 2 && args.length != 3) {
				System.err.println("Usage: ExperimentCache merge cacheFileToExtractDesignsFrom outputMergeFile [cacheFileToAddDesignsTo] ");
				return;
			}
			String cacheFileToExtractDesignsFrom = args[0];
			String outputMergeFile = args[1];
			String cacheFileToAddDesignsTo = (args.length==3 ? args[2] : null);
			
			ExperimentCache cacheToExtractDesignsFrom = ExperimentCache.loadCacheFromFile(cacheFileToExtractDesignsFrom);
			
			ExperimentCache cacheToAddDesignsTo;
			if (cacheFileToAddDesignsTo==null)
				cacheToAddDesignsTo = new ExperimentCache(outputMergeFile, 100, 1, 1, null);
			else {
				cacheToAddDesignsTo = ExperimentCache.loadCacheFromFile(cacheFileToAddDesignsTo);
			}
			
			cacheToAddDesignsTo.mergeDesigns(cacheToExtractDesignsFrom);
			cacheToAddDesignsTo.saveTheEntireCache(outputMergeFile);
			System.out.println("Designs from " + cacheFileToExtractDesignsFrom + " were successfully added to " + cacheFileToAddDesignsTo + " and saved in " + outputMergeFile);
		} else if (args[0].equals("transform")) {
			if (args.length != 3) {
				System.err.println("Usage: ExperimentCache transform cacheFileInOldFormat filenameForNewFormatCache ");
				return;
			}
			String oldFormat = args[1];
			String newFormat = args[2];
			transformOldCacheFormatIntoNewOne(oldFormat, newFormat);
			System.out.println("\n\nWe successfully transformed your old cache from " + oldFormat + " into a new file called " + newFormat);

		} else if (args[0].equals("renameAlgorithm")) {
			if (args.length != 4) {
				System.err.println("Usage: ExperimentCache renameAlgorithm cacheFilename oldName newName ");
				return;
			}
			String cacheFilename = args[1];
			String oldAlgName = args[2];
			String newAlgName = args[3];
			ExperimentCache cache = ExperimentCache.loadCacheFromFile(cacheFilename);
			cache.renameAlgorithmNames(oldAlgName, newAlgName);
			cache.saveTheEntireCache();
			System.out.println("\n\nWe successfully renamed all your algorithm names!");
			
		} else {
			throw new Exception("Unrecognized command: " + args[0]);
		}

	}
	

	
}
