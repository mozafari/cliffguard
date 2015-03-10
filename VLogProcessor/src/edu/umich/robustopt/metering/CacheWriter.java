package edu.umich.robustopt.metering;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.physicalstructures.DeployedPhysicalStructure;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.BLog.LogLevel;

	
public class CacheWriter implements Serializable {
	private static final long serialVersionUID = 5818938705919962952L;

	private String directoryPath = null;
	transient private int cdpIdx = 0;
	transient private int cdIdx = 0;
	transient private int cpIdx = 0;
	
	public CacheWriter(String directoryPath) throws Exception {
		/*
		if (directoryPath != null) {
			File path = new File(directoryPath);
			if (path.exists())
				throw new Exception("Cannot create a cache writer, as this directory already exists:" + directoryPath);
			else if (!path.mkdir())
				throw new Exception("Could not create directory " + path);
		}		
		this.directoryPath = directoryPath;
		*/
	}


	public void save_cacheDeployedProjections(Set<DeployedPhysicalStructure> deployedProjections) throws IOException {
		if (directoryPath == null)
			return;
		
		CacheDeployedProjections cdp = new CacheDeployedProjections(deployedProjections);
		cdp.save(directoryPath + "/CacheDeployedProjections" + (cdpIdx++));
	}

	public void save_cacheDesign(DesignKey key,	PhysicalDesign design) throws IOException {
		if (directoryPath == null)
			return;
		
		CacheDesign cd = new CacheDesign(key, design);
		cd.save(directoryPath + "/CacheDesign" + (cdIdx ++));
		
	}

	public void save_cachePerformance(String query, PhysicalDesign allowedProjections, PerformanceValue performanceValue) throws IOException {
		if (directoryPath == null)
			return;
		
		CachePerformance cp = new CachePerformance(query, allowedProjections, performanceValue);
		cp.save(directoryPath + "/CachePerformance" + (cpIdx  ++));	
	}


	public static void mergePartialCache(String originalExperimentalCache, String directoryNameForPartialResults, String outputExperimentCacheFilename) throws Exception {
		ExperimentCache experimentCache;
		if (originalExperimentalCache!=null)
			experimentCache = ExperimentCache.loadCacheFromFileForMerging(originalExperimentalCache);
		else
			experimentCache = new ExperimentCache(originalExperimentalCache, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
		
		if (experimentCache == null)
			throw new Exception("Failed to create an ExperimentCache object!");
		
		System.out.println("Original cache created, now going to replay all the logs!");
		
		// CacheDeployedProjections
		System.out.println("Loading CacheDeployedProjections: ");
		for (int i=0; true; ++i) {
			try {
				String filename = directoryNameForPartialResults + "/CacheDeployedProjections" + i;
				CacheDeployedProjections cdp = (CacheDeployedProjections) load(filename);
				experimentCache.cacheDeployedPhysicalStructures(cdp.deployedProjections);
				System.out.print(i + " ");
			} catch (IOException e) {
				System.out.println("Last CacheDeployedProjections index loaded: " + (i-1));
				break;
			}
		}
		// CacheDesign
		System.out.println("Loading CacheDesign: ");
		for (int i=0; true; ++i) {
			try {
				String filename = directoryNameForPartialResults + "/CacheDesign" + i;
				CacheDesign cd = (CacheDesign) load(filename);
				experimentCache.cacheDesign(cd.key, cd.design);
				System.out.print(i + " ");
			} catch (IOException e) {
				System.out.println("Last CacheDesign index loaded: " + (i-1));
				break;
			}
		}
		// CachePerformance
		System.out.println("Loading CachePerformance: ");
		for (int i=0; true; ++i) {
			try {
				String filename = directoryNameForPartialResults + "/CachePerformance" + i;
				CachePerformance cp = (CachePerformance) load(filename);
				experimentCache.cachePerformance(cp.query, cp.allowedStructures, cp.performanceValue);
				System.out.print(i + " ");
			} catch (IOException e) {
				System.out.println("Last CachePerformance index loaded: " + (i-1));
				break;
			}
		}
		System.out.println("Fully loaded, now going to write it all back to disk!");
		experimentCache.saveTheEntireCache(outputExperimentCacheFilename);
		System.out.println("The mergedFile was successfully written to " + outputExperimentCacheFilename);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: topDirectory originalExperimentFilename partialResultsSubDirectory outputFileName");
			return;
		} 
		
		String topDir = args[0] + "/";
		String originalExperimentFilename = args[1];
		String partialResultsSubDirectory = args[2];
		String outputFileName = args[3];
		
		mergePartialCache(topDir + originalExperimentFilename, topDir + partialResultsSubDirectory, topDir + outputFileName);
	}
	
	public static Saver load(String filename) throws ClassNotFoundException, IOException {
		Saver obj = null;
		ObjectInputStream in;
		in = new ObjectInputStream(new FileInputStream(new File(filename)));
		obj = (Saver) in.readObject();
		in.close();
		return obj;		
	}

	private class CacheDeployedProjections extends Saver implements Serializable {
		private static final long serialVersionUID = 7944336263192280919L;
		public Set<DeployedPhysicalStructure> deployedProjections;
		public CacheDeployedProjections(Set<DeployedPhysicalStructure> deployedProjections) {
			this.deployedProjections = deployedProjections;
		}
		
	}
	
	private class CacheDesign extends Saver implements Serializable {
		private static final long serialVersionUID = -4148143458825163717L;
		public DesignKey key;
		public PhysicalDesign design;

		public CacheDesign(DesignKey key, PhysicalDesign design) {
			this.key = key;
			this.design = design;
		}
	}

	private class CachePerformance extends Saver implements Serializable {
		private static final long serialVersionUID = -5928284699059947981L;
		public String query;
		public PhysicalDesign allowedStructures;
		public PerformanceValue performanceValue;

		public CachePerformance(String query, PhysicalDesign allowedStructures, PerformanceValue performanceValue) {
			this.query = query;
			this.allowedStructures = allowedStructures;
			this.performanceValue = performanceValue;
		}
	}
	
	private class Saver implements Serializable {
		private static final long serialVersionUID = -9120382373344476301L;

		public void save(String filename) throws IOException {
			ObjectOutputStream oos = null;
			RandomAccessFile raf = new RandomAccessFile(filename, "rw");
			FileOutputStream fos = new FileOutputStream(raf.getFD());
			oos = new ObjectOutputStream(new BufferedOutputStream(fos));
			oos.writeObject(this);
			oos.close();

		}
	}
	
}
