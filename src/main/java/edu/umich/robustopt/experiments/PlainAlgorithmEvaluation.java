package edu.umich.robustopt.experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.TwoWayMap;
import edu.umich.robustopt.workloads.DistributionDistance;



public class PlainAlgorithmEvaluation implements Serializable {

	private static final long serialVersionUID = -1286105629936045299L;
	private List<List<PerformanceRecord>> finalWindowsQueryPerformance = null;
	
	private List<List<String>> originalWindowsOfQueryStrings;
	private Map<String, List<PhysicalDesign>> algorithmsWindowsDesigns;
	private Map<String, List<Double>> algorithmsWindowsDesignsDisk;
	private Map<String, List<List<PerformanceRecord>>> algorithmsWindowsQueryPerformance;
	
	public void setFinalWindowsQueryPerformance(List<List<PerformanceRecord>> finalWindowsQueryPerformance) {
		this.finalWindowsQueryPerformance = finalWindowsQueryPerformance;
	}
	
	public PlainAlgorithmEvaluation(String algorithmName, 
			List<List<String>> originalWindowsOfQueryStrings,
			List<PhysicalDesign> windowsDesigns,
			List<Double> windowsDesignsDisks,
			List<List<PerformanceRecord>> windowsQueriesPerformance) {
		
		this.originalWindowsOfQueryStrings = originalWindowsOfQueryStrings;
		algorithmsWindowsDesigns = new HashMap<String, List<PhysicalDesign>>();
		algorithmsWindowsDesignsDisk = new HashMap<String, List<Double>>();
		algorithmsWindowsQueryPerformance = new HashMap<String, List<List<PerformanceRecord>>>();
						
		algorithmsWindowsDesigns.put(algorithmName, windowsDesigns);
		algorithmsWindowsDesignsDisk.put(algorithmName, windowsDesignsDisks);
		algorithmsWindowsQueryPerformance.put(algorithmName, windowsQueriesPerformance);
	}

	public void merge(PlainAlgorithmEvaluation other) throws Exception {
		if (this.finalWindowsQueryPerformance != null || other.finalWindowsQueryPerformance != null) 
			throw new Exception("You cannot merge finalized plain evaluation results!");
		
		if (!this.getOriginalWindowsOfQueryStrings().equals(other.getOriginalWindowsOfQueryStrings()))
			throw new Exception("You cannot merge two plain evaluations when they are regarding two different sets of windows! " + originalWindowsOfQueryStrings.size() + " and " + other.getOriginalWindowsOfQueryStrings().size());			
		
		this.algorithmsWindowsDesigns.putAll(other.algorithmsWindowsDesigns);
		this.algorithmsWindowsDesignsDisk.putAll(other.algorithmsWindowsDesignsDisk);
		this.algorithmsWindowsQueryPerformance.putAll(other.algorithmsWindowsQueryPerformance);		
	}
	
	public List<List<PerformanceRecord>> getFinalWindowsQueryPerformance() {
		return finalWindowsQueryPerformance;
	}

	public void writeToFile(String filename) throws FileNotFoundException, IOException {
		System.out.println("Writing to result file: " + filename);
		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(new FileOutputStream(new File(filename)));
		oos.writeObject(this);
		oos.close();
	}
	
	public static PlainAlgorithmEvaluation loadEvaluationFromFile(String filename) throws ClassNotFoundException {
		PlainAlgorithmEvaluation algEval = null;
		ObjectInputStream in;
		try {
			in = new ObjectInputStream(new FileInputStream(new File(filename)));
			try {
				algEval = (PlainAlgorithmEvaluation) in.readObject();
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return algEval;		
	}

	public Map<String, List<PhysicalDesign>> getAlgorithmsWindowsDesigns() {
		return algorithmsWindowsDesigns;
	}

	public Map<String, List<Double>> getAlgorithmsWindowsDesignsDisk() {
		return algorithmsWindowsDesignsDisk;
	}

	public Map<String, List<List<PerformanceRecord>>> getAlgorithmsWindowsQueryPerformance() {
		return algorithmsWindowsQueryPerformance;
	}

	public List<List<String>> getOriginalWindowsOfQueryStrings() {
		return originalWindowsOfQueryStrings;
	}

	
}
