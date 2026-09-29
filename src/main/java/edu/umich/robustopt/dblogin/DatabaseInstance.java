package edu.umich.robustopt.dblogin;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;


import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;

public interface DatabaseInstance {
	public Set<DeployedPhysicalStructure> getCurrentlyDeployedStructures() throws Exception;
	
	public boolean exportStatistics(String statsFileName) throws SQLException;
}
