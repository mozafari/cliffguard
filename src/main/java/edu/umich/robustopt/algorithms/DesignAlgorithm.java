package edu.umich.robustopt.algorithms;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.DBDeployer;
import edu.umich.robustopt.dbd.DBDesigner;
import edu.umich.robustopt.dbd.DesignParameters;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.metering.PerformanceRecord;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.workloads.DistributionDistance;

/*
 * This is the most generic class of design algorithms. It has no design signature (as it depends on whether it is an
 * ideal or realistic designer. Thus, this class only comes with a few common utilities.
 */
public abstract class DesignAlgorithm {
	protected BLog log;
	protected DBDesigner dbDesigner;
	protected DBDeployer dbDeployer;
	protected DesignParameters designParameters;
	protected ExperimentCache experimentCache;
	
	public DesignAlgorithm(LogLevel verbosity, DBDesigner dbDesigner, DBDeployer dbDeployer, DesignParameters designParameters, ExperimentCache experimentCache) {
		this.log = new BLog(verbosity);
		this.dbDesigner = dbDesigner;
		this.dbDeployer = dbDeployer;
		this.designParameters = designParameters;
		this.experimentCache = experimentCache;
	}
	
	public static List<String> convertPerformanceRecordToQueries (List<PerformanceRecord> windowPerformanceRecords) {
		List<String> windowQueries = new ArrayList<String>();
		for (int q=0; q<windowPerformanceRecords.size(); ++q)
			windowQueries.add(windowPerformanceRecords.get(q).getQuery());
		
		return windowQueries;
	}
	
	public static String computeSignatureString(String algorithmBasename, String algorithmSignature) {
		String result = (algorithmBasename==null ? "null" : algorithmBasename);
		result += (algorithmSignature==null ? "" : "::" + algorithmSignature);
		return result;
	}
	
	public static String computeAlgorithmRetrievalKey(DesignAlgorithm designAlgorithm, DistributionDistance distributionDistance) {
		String algorithmSignature = null;
		if (designAlgorithm instanceof RealisticDesignAlgorithm)
			algorithmSignature = (designAlgorithm instanceof RobustDesigner? 
										((RobustDesigner)designAlgorithm).robustSignature(distributionDistance) 
											: 
										((RealisticDesignAlgorithm)designAlgorithm).signature()
									);
		String algorithmRetrivalKey = computeSignatureString(designAlgorithm.getName(), algorithmSignature);

		return algorithmRetrivalKey;
	}
	
	public String getName() {
		return this.getClass().getSimpleName();
	}
	
	@Override
	final public String toString() {
		return getName();
	}
	
	protected String summarizeDifferenceBetweenTwoDesigns(PhysicalDesign leftDesign, PhysicalDesign rightDesign) throws Exception {
		Set<PhysicalStructure> leftSet = new HashSet<PhysicalStructure>(leftDesign.getPhysicalStructures());
		Set<PhysicalStructure> rightSet = new HashSet<PhysicalStructure>(rightDesign.getPhysicalStructures());
		
		Set<PhysicalStructure> leftUnique = new HashSet<PhysicalStructure>(leftSet);
		leftUnique.removeAll(rightSet);
		Set<PhysicalStructure> rightUnique = new HashSet<PhysicalStructure>(rightSet);
		rightUnique.removeAll(leftSet);
		Set<PhysicalStructure> commonSet = new HashSet<PhysicalStructure>(leftSet);
		commonSet.removeAll(leftUnique);
		
		String summary  = "<" + leftUnique.size() + "," + dbDeployer.retrieveDesignDiskSizeInGigabytes(new ArrayList<PhysicalStructure>(leftUnique)) + ">" 
					 	+ "<" + commonSet.size() + "," + dbDeployer.retrieveDesignDiskSizeInGigabytes(new ArrayList<PhysicalStructure>(commonSet)) + ">" 
					 	+ "<" + rightUnique.size() + "," + dbDeployer.retrieveDesignDiskSizeInGigabytes(new ArrayList<PhysicalStructure>(rightUnique)) + ">" 
						;
		
		return summary;
	}

	public String show() {
		return this.getClass().getSimpleName();
	}

	public DBDesigner getDbDesigner() {
		return dbDesigner;
	}

	public DBDeployer getDbDeployer() {
		return dbDeployer;
	}

	public DesignParameters getDesignParameters() {
		return designParameters;
	}
	
	
}
