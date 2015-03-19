package edu.umich.robustopt.workloads;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Clustering_QueryEquality;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.common.GlobalConfigurations;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.MyMathUtils;
import edu.umich.robustopt.util.SchemaUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Vector;
import java.util.Set;
import java.lang.Math;
import java.io.Serializable;

import sun.util.logging.resources.logging;

import com.relationalcloud.tsqlparser.loader.Schema;

public abstract class EuclideanDistance extends DistributionDistance implements Serializable{
	protected static final long serialVersionUID = 4907194932028227332L;
	protected Double euclideanDistance = Double.NaN;
	protected Double penaltyForGoingFromZeroToNonZero = 1d;
	protected Integer howManyPairsAreRepresentedByThisObject = 0;

	public abstract EuclideanDistance makeCopy(Double newDistanceValue) throws Exception;
	
	public EuclideanDistance(Double dist, Double penaltyForGoingFromZeroToNonZero, Integer numOfPairsRepresentedByThisObject) throws Exception{
		if (penaltyForGoingFromZeroToNonZero < 1d) {
			throw new Exception("Penalty factor should be larger than or equal to 1");
		}
		if (dist < 0d) {
			throw new Exception("Distance should be positive");
		}
		if (numOfPairsRepresentedByThisObject < 1) {
			throw new Exception("It should represent at least one euclidean distance");
		}
		this.euclideanDistance = dist;
		this.penaltyForGoingFromZeroToNonZero = penaltyForGoingFromZeroToNonZero;
		this.howManyPairsAreRepresentedByThisObject = numOfPairsRepresentedByThisObject;
	}
	
	public EuclideanDistance(Double dist, Double penaltyForGoingFromZeroToNonZero) throws Exception {
		this(dist, penaltyForGoingFromZeroToNonZero, 1);
	}
	
	public EuclideanDistance(EuclideanDistance dist) throws Exception{
		this(dist.euclideanDistance,  dist.penaltyForGoingFromZeroToNonZero, dist.howManyPairsAreRepresentedByThisObject);
	}//copy constructor
	
	public abstract  Double getDistance();
	
	public Double getPenaltyForGoingFromZeroToNonZero(){
		return penaltyForGoingFromZeroToNonZero;
	}
	
	@Override
	public int compareTo(DistributionDistance o) {
		if (o instanceof EuclideanDistance) {
			EuclideanDistance other = (EuclideanDistance)o;
			return Double.compare(this.getDistance(), other.getDistance());
		} else
			return this.showSummary().compareTo(o.showSummary());
	}

	
	public static abstract class Generator implements DistributionDistanceGenerator<EuclideanDistance> {
		protected List<ColumnDescriptor> dbColumns = new ArrayList<ColumnDescriptor>();
		protected Double generatorPenaltyForGoingFromZeroToNonZero = 1d;
		
		public Generator(Map<String, Schema> schemaMap, Double penaltyFactor) throws Exception{
			Integer count = new Integer(0);
			if (penaltyFactor < 1d) {
				throw new Exception("Penalty factor should be larger than or equal to 1");
			}
			this.generatorPenaltyForGoingFromZeroToNonZero = penaltyFactor;
			if (schemaMap != null){
				for (String schemaName : schemaMap.keySet()) {
					Schema schema = schemaMap.get(schemaName);
					for (String tableName : schema.getAllTables())
						for (String columnName : schema.getTable(tableName).getColumns()) {
							ColumnDescriptor col = new ColumnDescriptor (schemaName, tableName, columnName);
							dbColumns.add(count, col);
							count++;
						}
				}
			} else {
				throw new Exception("schemaMap should not be null");
			}
		}
		
		public Double getPenaltyForGoingFromZeroToNonZero() {
			return generatorPenaltyForGoingFromZeroToNonZero;
		}
		
		//protected abstract EuclideanDistance distance(ClusteredWindow leftWindow, ClusteredWindow rightWindow) throws Exception;
		
		protected HashMap<Vector<Boolean>, Double> convertClusteredWindowToHashMap(ClusteredWindow W) throws Exception{
			int size = W.numberOfClusters();
			Double fraction;
			HashMap<Vector<Boolean>, Double> result = new HashMap<Vector<Boolean>, Double>(size);
			Set<Cluster> clusters = W.getClusters();
			for (Cluster c : clusters){
				fraction = W.getFraction(c);
				result.put(convertClusterToBinaryVector(c), fraction);
			}
			return result;
		}
		
		protected abstract Vector<Boolean> convertClusterToBinaryVector(Cluster c) throws Exception;
		
		private int findBitmapSize(Map<Vector<Boolean>, Double> X, Map<Vector<Boolean>, Double> Y) throws Exception {
			int bitmapSize = -1;
			for (Vector<Boolean> key : X.keySet())
				if (bitmapSize == -1)
					bitmapSize = key.size();
				else if (bitmapSize != key.size())
					throw new Exception("The two arguments have used different bit-map sizes: " + bitmapSize + " and " + key.size());
			
			for (Vector<Boolean> key : Y.keySet())
				if (bitmapSize == -1)
					bitmapSize = key.size();
				else if (bitmapSize != key.size())
					throw new Exception("The two arguments have used different bit-map sizes: " + bitmapSize + " and " + key.size());
			
			if (bitmapSize==-1)
				throw new Exception("Empty arguments: bitmap size==-1");
			
			return bitmapSize;
		}
		
		protected Double getEuclideanDistance(Map<Vector<Boolean>, Double> X, Map<Vector<Boolean>, Double> Y) throws Exception{
			// first make sure that the bit vectors in the two arguments have the same number of bits!
			int bitmapSize = findBitmapSize(X, Y);
			
			Double subsum;
			Double sum = 0d;
			Double difference;
			Vector<Boolean> col;
			HashMap<Vector<Boolean>, Double> XminusY = new HashMap<Vector<Boolean>, Double>(X);
			for (Map.Entry<Vector<Boolean>, Double> entry: Y.entrySet()) {
				Vector<Boolean> Key = entry.getKey();
				if (XminusY.containsKey(Key)) {
					difference =  (Double) XminusY.get(Key);
				} else {
					difference = 0d;
				}
				XminusY.put(Key, Math.abs(difference - entry.getValue()));
			}
		
			Iterator< Map.Entry<Vector<Boolean>, Double> > it = XminusY.entrySet().iterator();
			if (generatorPenaltyForGoingFromZeroToNonZero == 1d) {
				while (it.hasNext()) {
					Map.Entry<Vector<Boolean>, Double>entry=(Map.Entry<Vector<Boolean>, Double>) it.next();
					col = entry.getKey();
					subsum = 0d;
					for (Map.Entry<Vector<Boolean>, Double> entry1: XminusY.entrySet())
						{
							subsum += entry1.getValue() * similarityMatrix(entry1.getKey(), col, bitmapSize);
						}
					sum += 2 * subsum * entry.getValue();
					it.remove();
				}
				return sum / (bitmapSize * 2);
			} else {
				for (Map.Entry<Vector<Boolean>, Double> entry: XminusY.entrySet()) {
					col = entry.getKey();
					subsum = 0d;
					for (Map.Entry<Vector<Boolean>, Double> entry1: XminusY.entrySet())
					{
						subsum += entry1.getValue() * similarityMatrixWithPenalty(entry1.getKey(), col, bitmapSize);
					}
					sum += subsum * entry.getValue();
				}
				return sum / ((1 + generatorPenaltyForGoingFromZeroToNonZero) * 2);
			}
		}
	
		protected int getNumOfColumns() {
			return dbColumns.size();
		}
		
		protected int similarityMatrix(Vector<Boolean> a, Vector<Boolean> b, int bitmapSize) {//a b has same size
			int count = 0;

			for (int i = 0; i < bitmapSize; ++i) {
			    if (a.get(i) != b.get(i)) {
			        count++;
			    }
			}
			return count;
		}
		
		protected double similarityMatrixWithPenalty(Vector<Boolean> a, Vector<Boolean> b, int bitmapSize) {//a b has same size
			int numOf1Inb = 0;
			int numOf0Inb = 0;
			int numOf1CoveredBya = 0;
			int numOf0CoveredBya = 0;

			for (int i = 0; i < bitmapSize; ++i) {
			    if (b.get(i) == true) {
			        numOf1Inb++;
			        if (a.get(i) == true) {
			        	numOf1CoveredBya++;
			        }
			    } else {
			    	numOf0Inb++;
			    	if (a.get(i) == false) {
			    		numOf0CoveredBya++;
			    	}
			    }
			}
			
			return (generatorPenaltyForGoingFromZeroToNonZero * numOf1CoveredBya / numOf1Inb)
					+ (numOf0CoveredBya / numOf0Inb);
		}
	}
	
}



