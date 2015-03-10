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
import edu.umich.robustopt.util.TwoWayMap;
import edu.umich.robustopt.workloads.DistributionDistance;



public class AlgorithmEvaluation implements Serializable {

	private static final long serialVersionUID = -1286105629936045299L;
	private Map<DistributionDistance, List<List<PerformanceRecord>>> finalDvalsWindowsQueryPerformance = null;
	
	private TwoWayMap<DistributionDistance, String, List<PhysicalDesign>> dvalsAlgorithmsWindowsDesigns;
	private TwoWayMap<DistributionDistance, String, List<Double>> dvalsAlgorithmsWindowsDesignsDisk;
	private TwoWayMap<DistributionDistance, String, List<List<PerformanceRecord>>> dvalsAlgorithmsWindowsQueryPerformance;
	
	public void setFinalDvalsWindowsQueryPerformance(Map<DistributionDistance, List<List<PerformanceRecord>>> finalDvalsWindowsQueryPerformance) {
		this.finalDvalsWindowsQueryPerformance = finalDvalsWindowsQueryPerformance;
	}
	
	public AlgorithmEvaluation(DistributionDistance distributionDistance, String algorithmName, 
			List<PhysicalDesign> windowsDesigns,
			List<Double> windowsDesignsDisks,
			List<List<PerformanceRecord>> windowsQueriesPerformance) {
		dvalsAlgorithmsWindowsDesigns = new TwoWayMap<DistributionDistance, String, List<PhysicalDesign>>();
		dvalsAlgorithmsWindowsDesignsDisk = new TwoWayMap<DistributionDistance, String, List<Double>>();
		dvalsAlgorithmsWindowsQueryPerformance = new TwoWayMap<DistributionDistance, String, List<List<PerformanceRecord>>>();
				
		HashMap<String, List<PhysicalDesign>> algorithmsWindowsDesigns = new HashMap<String, List<PhysicalDesign>>();
		HashMap<String, List<Double>> algorithmsWindowsDesignsDisk = new HashMap<String, List<Double>>();
		HashMap<String, List<List<PerformanceRecord>>> algorithmsWindowsQueryPerformance = new HashMap<String, List<List<PerformanceRecord>>>();
		
		algorithmsWindowsDesigns.put(algorithmName, windowsDesigns);
		algorithmsWindowsDesignsDisk.put(algorithmName, windowsDesignsDisks);
		algorithmsWindowsQueryPerformance.put(algorithmName, windowsQueriesPerformance);

		dvalsAlgorithmsWindowsDesigns.put(distributionDistance, algorithmsWindowsDesigns);
		dvalsAlgorithmsWindowsDesignsDisk.put(distributionDistance, algorithmsWindowsDesignsDisk);
		dvalsAlgorithmsWindowsQueryPerformance.put(distributionDistance, algorithmsWindowsQueryPerformance);
	}

	public void merge(AlgorithmEvaluation other) throws Exception {
		if (this.finalDvalsWindowsQueryPerformance != null || other.finalDvalsWindowsQueryPerformance != null) 
			throw new Exception("You cannot merge finalized evaluation results!");
		
		this.dvalsAlgorithmsWindowsDesigns.putAll(other.dvalsAlgorithmsWindowsDesigns);
		this.dvalsAlgorithmsWindowsDesignsDisk.putAll(other.dvalsAlgorithmsWindowsDesignsDisk);
		this.dvalsAlgorithmsWindowsQueryPerformance.putAll(other.dvalsAlgorithmsWindowsQueryPerformance);		
	}
	
	public Map<DistributionDistance, List<List<PerformanceRecord>>> getFinalDvalsWindowsQueryPerformance() {
		return finalDvalsWindowsQueryPerformance;
	}

	public void writeToFile(String filename) throws FileNotFoundException, IOException {
		System.out.println("Writing to result file: " + filename);
		ObjectOutputStream oos = null;
		oos = new ObjectOutputStream(new FileOutputStream(new File(filename)));
		oos.writeObject(this);
		oos.close();
	}
	
	public static AlgorithmEvaluation loadEvaluationFromFile(String filename) throws ClassNotFoundException {
		AlgorithmEvaluation algEval = null;
		ObjectInputStream in;
		try {
			in = new ObjectInputStream(new FileInputStream(new File(filename)));
			try {
				algEval = (AlgorithmEvaluation) in.readObject();
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return algEval;		
	}

	public TwoWayMap<DistributionDistance, String, List<PhysicalDesign>> getDvalsAlgorithmsWindowsDesigns() {
		return dvalsAlgorithmsWindowsDesigns;
	}

	public TwoWayMap<DistributionDistance, String, List<Double>> getDvalsAlgorithmsWindowsDesignsDisk() {
		return dvalsAlgorithmsWindowsDesignsDisk;
	}

	public TwoWayMap<DistributionDistance, String, List<List<PerformanceRecord>>> getDvalsAlgorithmsWindowsQueryPerformance() {
		return dvalsAlgorithmsWindowsQueryPerformance;
	}

	
}
