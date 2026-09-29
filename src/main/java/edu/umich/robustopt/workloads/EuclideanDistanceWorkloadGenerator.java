package edu.umich.robustopt.workloads;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Random;
import java.util.Scanner;
import java.util.List;

import edu.umich.robustopt.clustering.Cluster;
import edu.umich.robustopt.clustering.ClusteredWindow;
import edu.umich.robustopt.clustering.Query;
import edu.umich.robustopt.clustering.Query_SWGO;
import edu.umich.robustopt.common.Randomness;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.util.ListUtils;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.loader.SchemaTable;
//extends Query_SWGO_WorkloadGenerator
public abstract class EuclideanDistanceWorkloadGenerator extends Synthetic_SWGO_WorkloadGenerator<EuclideanDistance>{
	protected HashMap<Integer,String> columnsAndIndex = null;
	protected Integer expectedNumOfClusters = 0;
	protected int expectedTotalFrequency = 0;
	protected Double penaltyForGoingFromZeroToNonZero = Double.NaN;
	protected Integer maxNumberOfAttemps = 0;
	
	public EuclideanDistanceWorkloadGenerator(Map<String, Schema> schemaMap, ConstantValueManager constManager, int numOfClusters, int totalFrequency, int numOfTrials) throws Exception{
		super(schemaMap, constManager);
		this.expectedNumOfClusters = numOfClusters;
		this.expectedTotalFrequency = totalFrequency;
		this.maxNumberOfAttemps = numOfTrials;
		buildHashMap();
	}
	
	public EuclideanDistanceWorkloadGenerator(String dbAlias, String databaseLoginFile, String DBVendor, int numOfClusters, int totalFrequency, int numOfTrials) throws Exception{
		super(dbAlias, DBVendor, databaseLoginFile);
		this.expectedNumOfClusters = numOfClusters;
		this.expectedTotalFrequency = totalFrequency;
		this.maxNumberOfAttemps = numOfTrials;
		buildHashMap();
	}
	
	public EuclideanDistanceWorkloadGenerator(Map<String, Schema> schema, String dbAlias, double samplingRate, File f, int numOfClusters, int totalFrequency, int numOfTrials) throws Exception{
		super(schema, dbAlias, samplingRate, f);
		this.expectedNumOfClusters = numOfClusters;
		this.expectedTotalFrequency = totalFrequency;
		this.maxNumberOfAttemps = numOfTrials;
		buildHashMap();
	}
	
	private void buildHashMap() throws Exception{
		columnsAndIndex = new HashMap<Integer,String>();
		Integer count = new Integer(0);
		for (String schemaName : schema.keySet()) {
			Schema oneSchema = schema.get(schemaName);
			for (String tableName : oneSchema.getAllTables())
				for (String columnName : oneSchema.getTable(tableName).getColumns()) {
					String uniqueName = schemaName + "." + tableName + "." + columnName;
					if (columnsAndIndex.containsValue(uniqueName))
						throw new Exception("the following column appears more than once: " + uniqueName);
					else{
						columnsAndIndex.put(count,uniqueName);
						count++;
					}
				}
		}
	}
	
	/*
	@Override
	public ClusteredWindow forecastNextWindow(ClusteredWindow curWindow,
			EuclideanDistance distance) throws Exception {
		int frequency;
		int totalFreqSoFar = 0;
		HashMap<Vector<Boolean>, Double> Workload = convertClusteredWindowToHashMap(curWindow);
		Double euclidDistance = distance.getDistance();
		HashMap<Vector<Boolean>, Double> result;

		if (distance.getPenaltyForGoingFromZeroToNonZero() == 0d)
			result = generateRandomWorkLoad1(Workload, euclidDistance);
		else{
			penaltyForGoingFromZeroToNonZero = distance.getPenaltyForGoingFromZeroToNonZero();
			result = generateRandomWorkLoad2(Workload, euclidDistance);
		}
		Set<Cluster> Clusters = new HashSet<Cluster>();
		int count = result.size();
		for (Map.Entry<Vector<Boolean>,Double> entry: result.entrySet()){
			//List<ColumnDescriptor> emptyList = new ArrayList<ColumnDescriptor>();
			Query_SWGO query = new Query_SWGO(emptyList, emptyList, getWhereColumns(entry.getKey()), emptyList, emptyList);
			Cluster cluster = new Cluster(query);
			if (count == 1) {//the last entry
				frequency = expectedTotalFrequency - totalFreqSoFar;
				if (frequency == 0)
					frequency++;
			}
			else {
				frequency = (int) (expectedTotalFrequency * entry.getValue());
				if (frequency == 0)
					frequency = 1;
				totalFreqSoFar += frequency;
			}
			Clusters.add(createClusterWithNewFrequency(cluster, frequency));
			//Clusters.add(genCluster(query,frequency));//an alternative way
			count--;
		}
		ClusteredWindow nextWindow = new ClusteredWindow(Clusters);
		return nextWindow;
	}*/
	
	protected Cluster genCluster(Query_SWGO Q, int frequency) throws CloneNotSupportedException{
		List<Query> listOfQueries = new ArrayList<Query>();
		for (int i = 1; i <= frequency; i++){
			listOfQueries.add(Q);
		}
		Cluster result = new Cluster(listOfQueries);
		return result;
	}
	
	/*
	protected Set<ColumnDescriptor> getWhereColumns(Vector<Boolean> column){
		int size = column.size();
		int i;
		ColumnDescriptor where;
		Set<ColumnDescriptor> wherecols = new HashSet<ColumnDescriptor>();
		for (i = 0; i < size; i++){
			if (column.get(i)){
				where = qualifiedNameToColumnDescriptor(columnsAndIndex.get(i));
				wherecols.add(where);
			}
		}
		return wherecols;
	}*/
	
	protected ColumnDescriptor qualifiedNameToColumnDescriptor(String s){
		//set a qualified column to ColumnDescriptor
		Scanner columnsScanner  = new Scanner(s);
		columnsScanner.useDelimiter("\\.");
		ColumnDescriptor result = new ColumnDescriptor(columnsScanner.next(), columnsScanner.next(), columnsScanner.next());
		columnsScanner.close();
		return result;
	}
	
	protected int getNumOfColumns(){
		return columnsAndIndex.size();
	}
	
	/*
	protected HashMap<Vector<Boolean> ,Double> convertClusteredWindowToHashMap(ClusteredWindow W) throws Exception{
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
	*/
	
	/*
	protected Vector<Boolean> convertClusterToBinaryVector(Cluster c) throws Exception{
		int size = getNumOfColumns();
		Vector<Boolean> result = new Vector<Boolean>(size);
		//initialize result
		
		Query q = c.retrieveAQueryAtPosition(0);
		Query_SWGO query;
		if (! (q instanceof Query_SWGO))
			throw new Exception("Current EuclideanDistance only supports Query_SWGO");
		else
			query = (Query_SWGO)q;

		//if (!(query.getGroup_by().isEmpty() && query.getOrder_by().isEmpty() && query.getSelect().isEmpty()))
			//throw new Exception("Group_by, Order_by, Select should be empty in current version of EuclideanDistance");
		String columnName;
		query = (Query_SWGO)q;
		Set<ColumnDescriptor> whereCol = query.getWhere();
		Set<String> whereQualified = new HashSet<String>();
		for (ColumnDescriptor col : whereCol){
			columnName = col.getQualifiedName();
			whereQualified.add(columnName);
		}
		
		for (int i = 0; i < size; i++){
			String column = columnsAndIndex.get(i);
			if (whereQualified.contains(column)){
				result.add(true);}
			else{
				result.add(false);}
		}
		return result;
	}
	*/
	
	protected HashMap<Vector<Boolean> ,Double> generateRandomWorkLoad1(HashMap<Vector<Boolean> ,Double> workload, Double distance) throws Exception{
		HashMap<Vector<Boolean> ,Double> result = null;
		int count = 0;
		int xsize = workload.size();
		if (distance <= 0.1f && xsize > 2){
			if (expectedNumOfClusters != xsize){
				expectedNumOfClusters = xsize;
				//System.out.println("distance<=0.1, automatically change number of nonzero queries to "+k);
			}
			while (count < maxNumberOfAttemps){
				result = generatorAlgorithm1ForSmallDistance(workload,distance,expectedNumOfClusters);
				count ++;
				if (result != null)
					break;
			}
		}
		else{
			while (count < maxNumberOfAttemps){
				result = generatorAlgorithm1(workload,distance,expectedNumOfClusters);
				count++;
				if (result != null)
					break;
			}
		}

		if (result == null){
			throw new Exception ("No solution in " + count + " trials");
		}
		else
			System.out.println("Found solution in " + count + " trials");
		return result;
	}
	
	protected HashMap<Vector<Boolean>, Double> generatorAlgorithm1(HashMap<Vector<Boolean>, Double> X, Double distance, int k)
	{
		//d1(result-X)=d
		//x+y=1-subsum
		Double d = distance * 2 * getNumOfColumns();
		Double arr[] = new Double[2];
		Double dYp,rhs1,rhs2,p;
		Double x = 0d, y = 0d;
		Double xparameter = 0d,yparameter = 0d,xyparameter = 0d;
		Double subsum = 0d;//sum of p
		Vector<Boolean> vec = new Vector<Boolean>(getNumOfColumns());
		Vector<Boolean> xkey = new Vector<Boolean>(getNumOfColumns());
		Vector<Boolean> ykey = new Vector<Boolean>(getNumOfColumns());
		HashMap<Vector<Boolean> ,Double> Yp = new HashMap<Vector<Boolean>, Double>(k - 2);
		HashMap<Vector<Boolean> ,Double> result = new HashMap<Vector<Boolean>, Double>(k);

		while(true){
			vec = genRandomBooleanVector();
			if (!X.containsKey(vec))
				break;
		}
		result.put(vec, 0d);
		xkey = vec;
		while(true){
			vec = genRandomBooleanVector();
			if (!X.containsKey(vec) && !result.containsKey(vec))
				break;
		}
		ykey = vec;
		result.put(vec, 0d);
		//finish setup 2 unknown queries
		int kk = k - 2;
		int ceil = (X.size() >= kk) ? kk : X.size();
		if (distance >= 0.2f)
			ceil = randomInteger(ceil);
		if (distance >= 0.9f)
			ceil = 0;
		int count = 0;
		for (Map.Entry<Vector<Boolean>,Double> entry: X.entrySet()){
			if (count == ceil)
				break;
			p = getRandomFraction(k - 2);
			subsum += p;
			result.put(entry.getKey(), p);
			Yp.put(entry.getKey(), p);
			kk--;
			count++;
		}
		subsum+=randomlyInsertQuery(kk, result, Yp);
		//finish setup k-2 queries

		rhs1 = 1 - subsum;
		dYp = 2 * getNumOfColumns() * getEuclideanDistance(Yp, X);//changed
		rhs2 = d - dYp;

		HashMap<Vector<Boolean>, Double> YpminusX = subtract(Yp, X);
		xparameter = getXParameter(YpminusX, xkey);
		yparameter = getYParameter(YpminusX, ykey);
		xyparameter = (double)differenceOf2QueryColumnSets(ykey, xkey) * 2.0f;

		if(! solveSystemOfQuadraticEqns(rhs1, rhs2, xparameter, xyparameter, yparameter, arr) ){
			return null;
		}
		x = arr[0];
		y = arr[1];
		result.put(xkey, x);
		result.put(ykey, y);
		return result;
	}

	protected HashMap<Vector<Boolean> ,Double> generatorAlgorithm1ForSmallDistance(HashMap<Vector<Boolean>, Double> X, Double d, int k)
	{	//X.size()>=2 and quite big (close to 2^getNumOfColumns())
		//higher change to get solution if d is small
		//+-d1(result-X)=d
		//x+y=1-subsum
		int i,r1,r2;
		Double arr[] = new Double[2];
		Double dYp,xvalue,yvalue,rhs1,rhs2,p;
		Double xparameter = 0d,yparameter = 0d,xyparameter = 0d,subsum = 0d;
		Double x = 0d, y = 0d;
		Vector<Boolean> xkey = new Vector<Boolean>(getNumOfColumns());
		Vector<Boolean> ykey = new Vector<Boolean>(getNumOfColumns());
		HashMap<Vector<Boolean> ,Double> Yp = new HashMap<Vector<Boolean> ,Double>(k - 2);
		HashMap<Vector<Boolean> ,Double> result = new HashMap<Vector<Boolean> ,Double>(k);
		r1 = randomInteger(X.size() - 1);
		r2 = randomInteger(X.size() - 1);
		while (r2 == r1){
			r2 = randomInteger(X.size() - 1);
		}
		i = 0;
		for (Map.Entry<Vector<Boolean>,Double> entry: X.entrySet()){
			if (i == r1){
				xkey = entry.getKey();
				result.put(xkey, entry.getValue());
				Yp.put(xkey, entry.getValue());
			}
			else if (i == r2){
				ykey = entry.getKey();
				result.put(ykey, entry.getValue());
				Yp.put(ykey, entry.getValue());
			}	
			i++;
		}
		//finish setup 2 unknown queries
		subsum = X.get(xkey) + X.get(ykey);
		Double top;
		int count = 1;
		int terminate = X.size() - 2;

		for (Map.Entry<Vector<Boolean>,Double> entry: X.entrySet()){
			if (entry.getKey() != xkey && entry.getKey() != ykey){
				if (count == terminate){
					top = entry.getValue() - 0.01f;
					p = (Double)(Randomness.randGen.nextDouble() * (top) + 0.01);
					result.put(entry.getKey(), p);
					Yp.put(entry.getKey(), p);
					subsum += p;
					break;
				}
				top = entry.getValue() - 0.01f;
				p = (Double)(Randomness.randGen.nextDouble() * (top) + 0.01);
				result.put(entry.getKey(), p);
				Yp.put(entry.getKey(), p);
				subsum += p;
				count++;
			}
		}
		//finish setup k-2 queries
		rhs1 = 1 - subsum;
		dYp = getEuclideanDistance(Yp,X) * 2 * getNumOfColumns();
		rhs2 = (Double)(d * 2 * getNumOfColumns() - dYp);

		HashMap<Vector<Boolean> ,Double> YpminusX = subtract(Yp,X);
		xvalue = YpminusX.remove(xkey);
		xparameter = getXParameter(YpminusX, xkey);
		YpminusX.put(xkey, xvalue);
		yvalue = YpminusX.remove(ykey);
		yparameter = getYParameter(YpminusX, ykey);
		YpminusX.put(ykey, yvalue);
		xyparameter = differenceOf2QueryColumnSets(ykey,xkey) * 2.0d;
		if(! solveSystemOfQuadraticEqns(rhs1, rhs2, xparameter, xyparameter, yparameter,arr) ){
			return null;
		}
		x = arr[0];
		y = arr[1];
		result.put(xkey, x + X.get(xkey));
		result.put(ykey, y + X.get(ykey));
		return result;
	}
	
	protected HashMap<Vector<Boolean> ,Double> generateRandomWorkLoad2(HashMap<Vector<Boolean>, Double> workload, Double distance)
	{
		//d2(result-X)=d
		//x+y=1-subsum
		HashMap<Vector<Boolean>, Double> result = null;
		int count = 0;
		while (count < maxNumberOfAttemps){
			result = generatorAlgorithm2(workload, distance, expectedNumOfClusters);
			count++;
			if (result != null)
				break;
		}
		if (result == null)
			System.out.println("No solution in " + count + " trials");
		else
			System.out.println("Found solution in " + count + " trials");
		return result;
	}
	
	protected HashMap<Vector<Boolean> ,Double> generatorAlgorithm2(HashMap<Vector<Boolean>, Double> X, Double d, int k)
	{
		Double arr[] = new Double[2];
		Double dYp,rhs1,rhs2,p;
		Double x = 0d, y = 0d;
		Double xparameter = 0d, yparameter = 0d, xyparameter = 0d;
		Double subsum = 0d;//sum of p
		Vector<Boolean> vec = new Vector<Boolean>(getNumOfColumns());
		Vector<Boolean> xkey = new Vector<Boolean>(getNumOfColumns());
		Vector<Boolean> ykey = new Vector<Boolean>(getNumOfColumns());
		HashMap<Vector<Boolean> ,Double> Yp = new HashMap<Vector<Boolean> ,Double>(k - 2);
		HashMap<Vector<Boolean> ,Double> result = new HashMap<Vector<Boolean> ,Double>(k);
		
		while(true){
			vec = genRandomBooleanVector();
			if (!X.containsKey(vec))
				break;
		}
		result.put(vec, 0.1d);
		xkey = vec;
		while(true){
			vec = genRandomBooleanVector();
			if (!X.containsKey(vec) && !result.containsKey(vec))
				break;
		}
		ykey = vec;
		result.put(vec, 0.1d);
		//finish setup 2 unknown queries
		int kk = k - 2;
		int ceil = (X.size() >= kk) ? kk: X.size();
		ceil = randomInteger(ceil);
		int count = 0;
		for (Map.Entry<Vector<Boolean>,Double> entry: X.entrySet()){
			if (count == ceil)
				break;
			p = getRandomFraction(k - 2);
			subsum += p;
			result.put(entry.getKey(), p);
			Yp.put(entry.getKey(), p);
			kk--;
			count++;
		}
		subsum += randomlyInsertQuery(kk, result, Yp);
		//finish setup k-2 queries
		int n = (int) (Math.pow(2, getNumOfColumns()) - 1);
		rhs1 = 1 - subsum;
		dYp = getEuclideanDistance(Yp, X);
		HashMap<Vector<Boolean> ,Double> resultcp = new HashMap<Vector<Boolean> ,Double>(result);
		HashMap<Vector<Boolean> ,Double> Xcp = new HashMap<Vector<Boolean> ,Double>(X);
		
		rhs2 = (Double) (d * (penaltyForGoingFromZeroToNonZero * n + 1) - penaltyForGoingFromZeroToNonZero * getSigma(resultcp, Xcp));
		rhs2 -= dYp;
		rhs2 *= 2 * getNumOfColumns();
		if (rhs2 < 0) return null;
		
		HashMap<Vector<Boolean> ,Double> YpminusX = subtract(Yp,X);
		xparameter = getXParameter(YpminusX, xkey);
		yparameter = getYParameter(YpminusX, ykey);
		xyparameter = (double) (differenceOf2QueryColumnSets(ykey,xkey) * 2);
		if(! solveSystemOfQuadraticEqns(rhs1, rhs2, xparameter, xyparameter, yparameter,arr) ){
			return null;
		}
		x=arr[0];
		y=arr[1];
		result.put(xkey, x);
		result.put(ykey, y);
		return result;
	}
	
	protected int getSigma(HashMap<Vector<Boolean>, Double> Xcopy, HashMap<Vector<Boolean>, Double> Ycopy){
		int sum = 0;
		for (Map.Entry<Vector<Boolean>,Double> Xentry: Xcopy.entrySet()){
			if ( !Ycopy.containsKey(Xentry.getKey()) ){//X has, Y doesn't
				sum++;
			}
			else//X,Y both have
				Ycopy.remove(Xentry.getKey());
		}
		sum += Ycopy.size();
		return sum;
	}
	
	protected int differenceOf2QueryColumnSets(Vector<Boolean> a,Vector<Boolean> b){//a b has same size
		int count = 0;

		for (int i = 0; i < getNumOfColumns(); ++i) {
		    if (a.get(i) != b.get(i)) {
		        count++;
		    }
		}
		return count;
	}
	
	protected HashMap<Vector<Boolean> ,Double> subtract(HashMap<Vector<Boolean>, Double> X, HashMap<Vector<Boolean>, Double> Y){
		//return X-Y
		Double value;
		HashMap<Vector<Boolean> ,Double> result = new HashMap<Vector<Boolean>, Double>(X);
		for (Map.Entry<Vector<Boolean>,Double> entry: Y.entrySet())
		{
			Vector<Boolean> Key = entry.getKey();
			if (result.containsKey(Key))
				value =  (Double) result.get(Key);
			else
				value = 0d;
			result.put(Key, Math.abs(value - entry.getValue()));

		}//end of for
		return result;
	}

	protected Double getEuclideanDistance(HashMap<Vector<Boolean>, Double> X, HashMap<Vector<Boolean>, Double> Y){
		Double subsum;
		Double sum = 0d;
		Vector<Boolean> col;
		HashMap<Vector<Boolean> ,Double> XminusY = new HashMap<Vector<Boolean> ,Double>();
		XminusY = subtract(X, Y);

		for (Map.Entry<Vector<Boolean>,Double> entry: XminusY.entrySet())
		{
			col = entry.getKey();
			subsum = 0d;
			for (Map.Entry<Vector<Boolean>,Double> entry1: XminusY.entrySet())
			{
				subsum += entry1.getValue() * differenceOf2QueryColumnSets(entry1.getKey(), col);
			}
			sum += subsum * entry.getValue();
		}
		return sum / (getNumOfColumns() * 2);
	}

	
	protected Double getXParameter(HashMap<Vector<Boolean>, Double> YpminusX, Vector<Boolean> xkey){
		Double xparameter = 0d;
		for (Map.Entry<Vector<Boolean>,Double> entry1: YpminusX.entrySet())
		{
			xparameter += entry1.getValue() * differenceOf2QueryColumnSets(xkey,entry1.getKey());
			xparameter += entry1.getValue() * differenceOf2QueryColumnSets(entry1.getKey(), xkey);
		}
		return xparameter;
	}

	protected Double getYParameter(HashMap<Vector<Boolean>, Double> YpminusX, Vector<Boolean> ykey){
		Double yparameter = 0d;
		for (Map.Entry<Vector<Boolean>,Double> entry1: YpminusX.entrySet())
		{
			yparameter += entry1.getValue() * differenceOf2QueryColumnSets(ykey,entry1.getKey());
			yparameter += entry1.getValue() * differenceOf2QueryColumnSets(entry1.getKey(),ykey);
		}
		return yparameter;
	}

	protected Double randomlyInsertQuery(int num, HashMap<Vector<Boolean>,Double> result, HashMap<Vector<Boolean>,Double> Yp){
		//used to insert num=k-2 queries with random value and random position
		//return Sum(p)
		Double subsum = 0d;
		Double p;
		Vector<Boolean> vec;
		int i;
		if (num <= 0)
			return 0d;
		for (i = 1; i <= num; i++){
			vec = genRandomBooleanVector();
			if (result.containsKey(vec)){//have already generated that key
				i--;
				continue;
			}
			p = getRandomFraction(num);
			subsum += p;
			result.put(vec, p);
			Yp.put(vec, p);
		}//finish setup k-2 queries
		return subsum;
	}

	protected Vector<Boolean> genRandomBooleanVector(){
		//random generate Vector<Boolean> with size getNumOfColumns()
		//every call should generate different result
		Boolean element;
		Vector<Boolean> result = new Vector<Boolean>(getNumOfColumns());
		Boolean done = false;
		int count = 0;
		while(!done)
		{
			count = 0;
			Vector<Boolean> temp = new Vector<Boolean>(getNumOfColumns());
			for(int i = 0; i < getNumOfColumns(); i++)
			{
				element = Randomness.randGen.nextBoolean();				
				temp.add(element);
				if(!element) count++;
			}
			if(count < getNumOfColumns()) {
				done = true;
				result = temp;
			}
		}
		return result;
	}

	protected int randomInteger(int k){
		//randomize an integer from 0 to k
		int result;
		result = Randomness.randGen.nextInt(k + 1);
		return result;
	}

	protected static Double getRandomFraction(int k) {
		// generate Double 0.01<=p<1/k
		Double result;
		Double floor =(double) 1 / k;
		floor -= 0.01f;
		result = (Double)(Randomness.randGen.nextDouble() * (floor) + 0.01);
		return result;
	}

	protected boolean solveSystemOfQuadraticEqns(Double a, Double b, Double k1, Double k2, Double k3, Double s[] ){
		//solve system of quadratic eqns
//		x+y=a
//		k1*x+k2*xy+k3*y=b
		//if no solution, return 0, else return 1
		//s[0]=x,s[1]=y
		Double a1,b1,c1;
		Double x1[] = new Double[2];
		boolean flag;
		a1 =- k2;
		b1 = k1 - k3 + a * k2;
		c1 = a * (k3 - k1) - b + k1 * a;
		flag =! solveQuadraticEqn(a1, b1, c1, x1);
		if (flag)
			return false;
		else{
			s[1] =- x1[0] + a;
			if (s[1] > 0 && x1[0] > 0){
				s[0] = x1[0];
				return true;
			}
			else{
				s[0] = x1[1];
				s[1] =- x1[1] + a;
				if (s[0] > 0 && s[1] > 0)
					return true;
				else
					return false;
			}
		}	
	}

	protected boolean solveQuadraticEqn(Double a, Double b, Double c, Double sln[]){
		//if delta < 0(no solution),return 0. else return 1
		//ax^2+bx+c=0
		double delta;
		delta = b * b - 4 * a * c;
		if(delta < 0)
			return false;
		else
		{
			sln[0] = (Double) (- b + Math.sqrt(delta)) / (2 * a);
			sln[1] = (Double) (- b - Math.sqrt(delta)) / (2 * a);
			return true;
		}
	}

	//copied from SimpleTPCHSyntheticWorkloadGenerator
	protected Query_SWGO GenerateRandomQuery() {
		// pick the number of predicates to go in the where clause
				// sample table uniformly at random:
				Schema s = ListUtils.PickOne(new ArrayList<Schema>(schema.values()));
				SchemaTable t = ListUtils.PickOne(new ArrayList<SchemaTable>(s.getTables()));
				List<ColumnDescriptor> whereCols = chooseUptoKColumns(t, 1);
				List<ColumnDescriptor> groupBy = chooseUptoKColumns(t, 2);
				List<ColumnDescriptor> selectCols = chooseUptoKColumns(t, 1);
				List<ColumnDescriptor> emptyCols = Collections.emptyList();
				Query_SWGO q = null;
				try {
					q = new Query_SWGO(MergeDescriptors(selectCols, groupBy), emptyCols, whereCols, groupBy, emptyCols);
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
				return q;
	}
	// The following 3 functions are needed in GenerateRandomQuery()
	
	private static List<ColumnDescriptor> MergeDescriptors(
			List<ColumnDescriptor> a,
			List<ColumnDescriptor> b) {
		//copied from SimpleTPCHSyntheticWorkloadGenerator
		Set<ColumnDescriptor> x = new HashSet<ColumnDescriptor>();
		x.addAll(a);
		x.addAll(b);
		return new ArrayList<ColumnDescriptor>(x);
	}

	/*
	@Override
	public DistributionDistanceGenerator<EuclideanDistance> getDistributionDistanceGenerator() throws Exception {
		return new EuclideanDistanceWithSimpleUnion.Generator(schema, penaltyForGoingFromZeroToNonZero, EuclideanDistanceWorkloadGeneratorFromLogFileWithSimpleUnionJingkui.AllClausesOption);
	}
	*/
}
