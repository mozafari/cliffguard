package edu.umich.robustopt.dbd;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.OutputStream;
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

import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DBInvoker;
import edu.umich.robustopt.dblogin.DatabaseInstance;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.dblogin.QueryPlanParser;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;
import edu.umich.robustopt.util.Timer;

public abstract class DBDeployer extends DBInvoker implements DatabaseInstance {	
	private Set<DeployedPhysicalStructure> currentDeployedStructs = null;
	private boolean deployMissingStructsDuringInitialization = false;
	private QueryPlanParser queryPlanParser = null;
	
	// book keeping
	private static HashMap<PhysicalStructure, String> deployCommandsMap = new HashMap<PhysicalStructure, String>();
	private static transient double secondsSpentRetrievingDiskSize = 0;
	private static transient long numberOfStructuresDeployed = 0;
	private transient long secondsSpentInitializing = 0;
	private transient long secondsSpentDeploying = 0;
	

	private transient DatabaseLoginConfiguration databaseLoginConfiguration;
	
	public DBDeployer (LogLevel verbosity, DatabaseLoginConfiguration databaseLoginConfiguration, ExperimentCache experimentCache, boolean deployMissingStructuresDuringInitialization) throws Exception {
		super(verbosity, databaseLoginConfiguration, experimentCache);
		this.databaseLoginConfiguration = databaseLoginConfiguration;
		this.deployMissingStructsDuringInitialization = deployMissingStructuresDuringInitialization;
	}
	
	/* this function is in charge of adding the statistics stored in a file (localPath) to a remote database
	 * which does not have any statistics simply because it is empty! I.e., the goal of this function is to 
	 * fool an empty DB to think that it has some data and statistics about that data, so that it finds a design
	 * that is optimal for that imaginary DB.
	 * Returns true upon success and false otherwise.
	 */
	public abstract boolean copyStatistics(DatabaseLoginConfiguration emptyDB, String localPath);
	
	public abstract QueryPlanParser createQueryPlanParser();

	public QueryPlanParser getQueryPlanParser() {
		if (queryPlanParser == null) {
			queryPlanParser = createQueryPlanParser();
		}
		return queryPlanParser;
	}

	public abstract Set<String> retrieveAllDeployedStructuresBaseNamesFromDB (Connection conn) throws SQLException;
	
	public abstract Set<DeployedPhysicalStructure> retrieveDeployedStructuresFromDB(Connection conn, 
				Set<String> allDeployedStructureBaseNamesInTheDatabase, Set<String> cachedDeployedStructureBaseNames) throws Exception;

	public abstract boolean wasCreatedByOurselves(DeployedPhysicalStructure deployedPhysicalStructure);
	
	public boolean dropAllStructures() throws Exception {
		boolean success = true;
		Set<DeployedPhysicalStructure> currentStructs = new HashSet<DeployedPhysicalStructure>();
		try {
			Set<DeployedPhysicalStructure> currentStructOrig = getCurrentlyDeployedStructures();
			//now make a copy so you delete from your set!
			currentStructs.addAll(currentStructOrig); 
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		log.status(LogLevel.STATUS, "Going to delete all existing structures !!! (there are " + currentStructs.size() + " of them) ...");
		for (DeployedPhysicalStructure p : currentStructs) {
			assert (wasCreatedByOurselves(p));
			if (!dropPhysicalStructureIfExists(dbConnection, p.getSchema(), p.getBasename())) {
				success = false;
				log.error("Could not delete this structure " + p.getBasename() );
			}
		}
		return success;				
	}
	
	public void dropAllStructuresExcept(Set<PhysicalStructure> physicalStructuresToExclude) throws Exception{
		Set<DeployedPhysicalStructure> deployedPhysicalStructures = new HashSet<DeployedPhysicalStructure>(getCurrentlyDeployedStructures());
		for (DeployedPhysicalStructure p : deployedPhysicalStructures) {
			if (!physicalStructuresToExclude.contains(p.getStructure()))
				dropPhysicalStructureIfExists(dbConnection, p.getSchema(), p.getBasename());
		}
	}

	// return true upon success
	public boolean dropPhysicalStructureIfExists(Connection conn, String schemaName, String structureName) {
		boolean found = false;
		for (DeployedPhysicalStructure dp : currentDeployedStructs)
			if (dp.getSchema().equals(schemaName) && dp.getBasename().equals(structureName)) {
				currentDeployedStructs.remove(dp);
				found = true;
				break;
			}
		
		if (!found)
			log.error("We dropped structure " + schemaName + " . " + structureName + " but did not find it in our currentDeployedStructs!");

		boolean result = dropPhysicalStructure(conn, schemaName, structureName);
		if (!result)
			return false;
		
		return found; 

	}
		
	protected abstract boolean dropPhysicalStructure(Connection conn, String schemaName, String structureName);

	// return the deployed structure if the structure had to be built from scratch, return null if the structure could not be built or already existsed
	public abstract DeployedPhysicalStructure deployStructure(PhysicalStructure structure) throws Exception;
	
	// return the number of new structures that had to be built from scratch and -1 when the process fails
	public int deployDesign(List<PhysicalStructure> structures, boolean dropOldOnes) throws Exception {			
		try {
			Timer t = new Timer();
			if (dropOldOnes)
				dropAllStructures();
			
			if (structures.size()==0) {
				log.status(LogLevel.VERBOSE, "No structures were given!");
				return 0;
			}
			
//				if (isDBempty())
//					throw new SQLException("Calling deployment on an empty DB?!");
			
			Timer outerT = new Timer();
						
			// check to see if this projection already exists by structure			
			Map<PhysicalStructure, DeployedPhysicalStructure> currentStructures = getStructureToDeployedMap();
			
			int newStructuresBuilt = 0;
			Double size_gb;
			for (PhysicalStructure struct : structures) {
				Timer innerT = new Timer();
				size_gb = null;
				try {
					DeployedPhysicalStructure existing = null;
					if ((existing = currentStructures.get(struct)) != null) {
						log.status(LogLevel.DEBUG, "[INFO] Already found a structure with name: " + existing.getStructureIdent().getQualifiedName());
						
						if (struct.getDiskSizeInGigabytes() == null)
							struct.setDiskSizeInGigabytes(retrieveStructureDiskSizeInGigabytes(dbConnection, existing.getSchema(), existing.getBasename()));
						
						continue;
					}
					++numberOfStructuresDeployed;
					
					// get new name
					DeployedPhysicalStructure deployedStruct = deployStructure(struct);
					if (deployedStruct == null)
						throw new Exception ("We have to understand why the return value is null!");
					++ newStructuresBuilt;
					size_gb = retrieveStructureDiskSizeInGigabytes(deployedStruct.getSchema(), deployedStruct.getBasename());					
					struct.setDiskSizeInGigabytes(size_gb);
					log.status(LogLevel.DEBUG, "Deploying structure took "+ innerT.lapMillis() + " ms and "+ (size_gb==null? "NULL" : size_gb.toString()) + " GB of disk space");
					
					currentDeployedStructs.add(deployedStruct);
					updateCache();
				} catch (Exception e) {
					log.error(e.getLocalizedMessage());
				}
			}
			log.status(LogLevel.VERBOSE, "Deploying all "+ structures.size()+" structures took "+ outerT.lapMillis() + " ms, built "+newStructuresBuilt+" new structures.");
			secondsSpentDeploying += t.lapSeconds();
			
			return newStructuresBuilt;
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
			return -1;
		}
	}

	// removing physical structures with the same structures but different names
	public void cleanUpDuplicatedStructures() throws Exception {
		if (currentDeployedStructs == null)
			return;
		
		Set<DeployedPhysicalStructure> duplicatedStructsToBeRemoved = findDuplicatedStructures(currentDeployedStructs);
		
		// now let us remove the duplicated ones! they exist in the DB because currentDeployedStructs is guaranteed to always reflect what's deployed in the DB
		for (DeployedPhysicalStructure p : duplicatedStructsToBeRemoved) {
			log.status(LogLevel.WARNING, "found a repeated deployed structure, going to remove it: " + p.getSchema() +"."+ p.getBasename());
			dropPhysicalStructure(dbConnection, p.getSchema(), p.getBasename());
		}		
	}

	public Set<DeployedPhysicalStructure> findDuplicatedStructures(Set<DeployedPhysicalStructure> deployedStructs) throws Exception {
		Set<DeployedPhysicalStructure> duplicatedStructsToBeRemoved = new HashSet<DeployedPhysicalStructure>();

		Set<PhysicalStructure> currentStructures = new HashSet<PhysicalStructure>();
		for (DeployedPhysicalStructure p : deployedStructs) {
			PhysicalStructure struct = p.getStructure();
			if (!currentStructures.contains(struct))
				currentStructures.add(struct);
			else { // we have two deployed structures with the same structure but installed under two different names!
				duplicatedStructsToBeRemoved.add(p);
			}
		}
		return duplicatedStructsToBeRemoved;
	}
	
	public Map<PhysicalStructure, DeployedPhysicalStructure> getStructureToDeployedMap() throws Exception {
		cleanUpDuplicatedStructures();
		Set<DeployedPhysicalStructure> deployedStructs = getCurrentlyDeployedStructures();

		Map<PhysicalStructure, DeployedPhysicalStructure> currentStructures = new HashMap<PhysicalStructure, DeployedPhysicalStructure>();
		for (DeployedPhysicalStructure p : deployedStructs) {
			PhysicalStructure struct = p.getStructure();
			currentStructures.put(struct, p);
		}
		
		return currentStructures;
	}

	private void updateCache() throws Exception {
		if (experimentCache!=null) {
			experimentCache.forgetAllDeployedPhysicalStructures();
			experimentCache.cacheDeployedPhysicalStructures(currentDeployedStructs);
			experimentCache.removeReferencesToStaleStructures();
		}
	}

	private void syncDeployedStructuresWithDB() throws Exception {
		Map<String, DeployedPhysicalStructure> basenameToDeployedStructure = new HashMap<String, DeployedPhysicalStructure>();

		// first add all the cached versions to our currentDeployedStructs
		if (currentDeployedStructs != null)
			for (DeployedPhysicalStructure struct : currentDeployedStructs)
				basenameToDeployedStructure.put(struct.getBasename(), struct);

		if (experimentCache!=null) 
			for (DeployedPhysicalStructure struct : experimentCache.getDeployedPhysicalStructures())
				if (!basenameToDeployedStructure.containsKey(struct.getBasename()))
					basenameToDeployedStructure.put(struct.getBasename(), struct);

		Set<String> allCachedStructureNames = basenameToDeployedStructure.keySet();
		
		log.statusNoNewLine(LogLevel.STATUS, "retrieving names of all deployed strcutures from DB ...");
		Timer t1 = new Timer();
		Set<String> allDeployedStructureNames = retrieveAllDeployedStructuresBaseNamesFromDB(dbConnection);
		log.status(LogLevel.STATUS, "retrieving names of all deployed strcutures from DB took: " + t1.lapMinutes() + " minutes.");

		Set<String> cachedButNotDeployedNames = new HashSet<String>(allCachedStructureNames); cachedButNotDeployedNames.removeAll(allDeployedStructureNames);
		Set<String> deployedButNotCachedNames = new HashSet<String>(allDeployedStructureNames); deployedButNotCachedNames.removeAll(allCachedStructureNames);
		log.status(LogLevel.WARNING, "cachedButNotDeployedNames=" + cachedButNotDeployedNames.size() + ", deployedButNotCachedNames=" + deployedButNotCachedNames.size());

		//1. Fetch structure of deployedButNotCachedNames structures
		log.status(LogLevel.STATUS, "retrieving deployed structures that are not cached from the database ...");
		Timer t = new Timer();
		Set<DeployedPhysicalStructure> newlyDeployedStructures = retrieveDeployedStructuresFromDB(dbConnection, allDeployedStructureNames, allCachedStructureNames);
		log.status(LogLevel.STATUS, "retrieving non-cached deployed structures from DB took: " + t.lapMinutes() + " minutes.");

		Set<DeployedPhysicalStructure> allDeployedStructures = new HashSet<DeployedPhysicalStructure>(newlyDeployedStructures);
		for (String structName : allCachedStructureNames)
			if (allDeployedStructureNames.contains(structName)) // we're taking the intersection of both cached and deployed structures here
				allDeployedStructures.add(basenameToDeployedStructure.get(structName));
		
		Set<PhysicalStructure> allStructuresThatAreDeployed = new HashSet<PhysicalStructure>();
		for (DeployedPhysicalStructure struct : allDeployedStructures)
			allStructuresThatAreDeployed.add(struct.getStructure());
		
		Set<PhysicalStructure> allMissingStructures = new HashSet<PhysicalStructure>();
		// find those cached but not deployed
		for (String structName : cachedButNotDeployedNames) {
			PhysicalStructure struct = basenameToDeployedStructure.get(structName).getStructure();
			if (!allStructuresThatAreDeployed.contains(struct))
					allMissingStructures.add(struct);
		}
		// find those structures refered to in our designs but not deployed
		for (PhysicalDesign design : experimentCache.getAllDesigns().values())
			for (PhysicalStructure struct : design.getPhysicalStructures())
				if (!allStructuresThatAreDeployed.contains(struct))
					allMissingStructures.add(struct);
		
		currentDeployedStructs = allDeployedStructures;
		// Now decide how to deal with those missing ones!
		if (deployMissingStructsDuringInitialization) {
			log.status(LogLevel.STATUS, "Going to create " + allMissingStructures.size() + " missing structures in the DB...");
			List<PhysicalStructure> missingStructuresList = new ArrayList<PhysicalStructure>(allMissingStructures);
			for (int i=0; i<missingStructuresList.size(); i+=10) {
				int j = (i+10 < missingStructuresList.size() ? i+10 : missingStructuresList.size());
				List<PhysicalStructure> design = missingStructuresList.subList(i, j);
				deployDesign(design, false);
				if (i % (int)(missingStructuresList.size()/100) == 0);
					log.statusNoNewLine(LogLevel.STATUS, (i / (int)(missingStructuresList.size()/100)) + "%");
			}
		}
		
		cleanUpDuplicatedStructures();
		updateCache();
		secondsSpentInitializing += t.lapSeconds();
	}
	
	public Set<DeployedPhysicalStructure> getCurrentlyDeployedStructures() throws Exception {
		if (currentDeployedStructs == null)
			syncDeployedStructuresWithDB();
		return currentDeployedStructs;
	}

	/* This function builds a hashMap using the currentDeployedStructs on-the-fly.
	 * WARNING: This Map is not kept up-to-date! So every time this map is used, we need to call this function again!
	 * 
	 */
	public Map<String, PhysicalStructure> getStructureBaseNameToStructureMap() throws Exception {
		Set<DeployedPhysicalStructure> deployedStructures = getCurrentlyDeployedStructures();
		
		Map<String, PhysicalStructure> ret = new HashMap<String, PhysicalStructure>();
		
		for (DeployedPhysicalStructure d : deployedStructures) {
			ret.put(d.getBasename(), d.getStructure());
		}
		
		return ret;
	}

	/* This function builds a hashMap using the currentDeployedStructs on-the-fly.
	 * WARNING: This Map is not kept up-to-date! So every time this map is used, we need to call this function again!
	 * 
	 */
	public Map<String, PhysicalStructure> getStructureNameToStructureMap() throws Exception {
		Set<DeployedPhysicalStructure> deployedStructures = getCurrentlyDeployedStructures();
		
		Map<String, PhysicalStructure> ret = new HashMap<String, PhysicalStructure>();
		
		for (DeployedPhysicalStructure d : deployedStructures) {
			ret.put(d.getName(), d.getStructure());
		}
		
		return ret;
	}
	
	public Double retrieveStructureDiskSizeInGigabytes(String structureSchema, String structureBaseName) throws SQLException {
		return retrieveStructureDiskSizeInGigabytes(dbConnection, structureSchema, structureBaseName);
	}
	
	public abstract Double retrieveStructureDiskSizeInGigabytes(Connection conn, String structureSchema, String structureBaseName) throws SQLException;
	
	public Double retrieveStructureDiskSizeInGigabytes(PhysicalStructure struct) throws Exception {
		if (struct.getDiskSizeInGigabytes() != null)
			return struct.getDiskSizeInGigabytes();
		
		Map<PhysicalStructure, DeployedPhysicalStructure> currentStructures = getStructureToDeployedMap(); 
						
		DeployedPhysicalStructure existing = currentStructures.get(struct);
		if (existing != null) {	
			Double sz = retrieveStructureDiskSizeInGigabytes(existing);
			struct.setDiskSizeInGigabytes(sz);
			return sz;
		} else {
			log.error("The requested structure is not currently deployed, cannot determined its size!");
			return null;
		}
	}

	public static void setDeployCommands(PhysicalStructure physicalStructure, String deployCommands) {
		deployCommandsMap.put(physicalStructure, deployCommands);
	}
	
	public static String getDeployCommands(PhysicalStructure physicalStructure) {
		String deployCommands = deployCommandsMap.get(physicalStructure);
		return deployCommands == null ? "" : deployCommands;
	}
	
	public Double retrieveStructureDiskSizeInGigabytes(DeployedPhysicalStructure deployedStruct) throws SQLException {
		NamedIdentifier namedIdentifier = deployedStruct.getStructureIdent();
		return retrieveStructureDiskSizeInGigabytes(namedIdentifier.first, namedIdentifier.second);		
	}
	
	public Double retrieveDesignDiskSizeInGigabytes(List<PhysicalStructure> design) throws Exception {
		Double totalStorage = 0.0;
		if (design == null) {
			return totalStorage;
		}
		for (PhysicalStructure struct : design) {
			Double size_gb = retrieveStructureDiskSizeInGigabytes(struct);
			if (size_gb != null)
				totalStorage += size_gb;
		}
		return totalStorage;
	}

	// responsible for exporting the DB stats and storing in a given file name
	public abstract boolean exportStatistics(String statsFileName) throws SQLException;
	
	public String reportStatistics() {
		String msg = "mins spent initializing=" + secondsSpentInitializing/60 + 
				", mins spent calculating disk size=" + secondsSpentRetrievingDiskSize/60 + 
				", mins spent deploying=" + secondsSpentDeploying/60 + ", numberOfStructuresDeployed=" + numberOfStructuresDeployed + "\n";
		return msg;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: DBDeployer originalCacheFile databaseConfigFile updatedCacheFile databaseName");
			return;
		}
		String originalCacheFile = args[0];
		String configFile = args[1];
		String updatedCacheFile = (args.length==3 ? args[2] : null);
		String databaseName = args[3];		
	}

}
