package edu.umich.robustopt.clustering;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import edu.umich.robustopt.common.BLog;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.workloads.DistributionDistance;
import edu.umich.robustopt.workloads.DistributionDistanceGenerator;

public abstract class QueryLogAnalyzer <Q extends Query>{
	static BLog log;
	protected DistributionDistanceGenerator<? extends DistributionDistance> distDistanceGenerator;

	public QueryLogAnalyzer(QueryParser<Q> qParser, DistributionDistanceGenerator<? extends DistributionDistance> distDistanceGenerator, 
			LogLevel level, String logFile) throws FileNotFoundException {
		if (logFile!=null)
			this.log = new BLog(logFile, logFile, level);
		else
			this.log = new BLog(level);
		this.distDistanceGenerator = distDistanceGenerator;
	}

	public QueryLogAnalyzer(QueryParser<Q> qParser, DistributionDistanceGenerator<? extends DistributionDistance> distDistanceGenerator, 
			LogLevel level) throws FileNotFoundException {
		this(qParser, distDistanceGenerator, level, null);
	}

	
	public List<DistributionDistance> measureDistancesBetweenPairsOfLaggedWindows(List<QueryWindow> queryWindowList, int lagBetweenPairsOfWindows) throws Exception {
		if (lagBetweenPairsOfWindows<1)
			throw new Exception("lagBetweenPairsOfWindows < 1 :" + lagBetweenPairsOfWindows);
		List<DistributionDistance> allDistances = new ArrayList<DistributionDistance>();
		
		for (int i=0; i<queryWindowList.size()-lagBetweenPairsOfWindows; ++i) {
			DistributionDistance d = distDistanceGenerator.distance(queryWindowList.get(i).getQueries(), queryWindowList.get(i+lagBetweenPairsOfWindows).getQueries());
			allDistances.add(d);
			log.status(LogLevel.VERBOSE, "====================\nWin"+ i + " had " + queryWindowList.get(i).getQueries().size()  + " queries, Distance between Win " + i + " and Win " + (i+lagBetweenPairsOfWindows) + " " + d.showSummary());
		}
		int last = queryWindowList.size()-lagBetweenPairsOfWindows;
		log.status(LogLevel.VERBOSE, "====================\nWin"+ last + " had " + queryWindowList.get(last).getQueries().size()  + " queries");
		

		return allDistances;
	}
	
	public DistributionDistance measureAvgDistanceBetweenPairsOfLaggedWindows(List<QueryWindow> queryWindowList, int lagBetweenPairsOfWindows) throws Exception {
		List<DistributionDistance> allDistances = measureDistancesBetweenPairsOfLaggedWindows(queryWindowList, lagBetweenPairsOfWindows); 
		
		DistributionDistance avgD = null; 
		for (int i=0; i<allDistances.size(); ++i) {
			DistributionDistance d = allDistances.get(i);
			if (avgD==null)
				avgD = d;
			else
				avgD = avgD.computeAverage(avgD, d);
		}
		return avgD;
	}

	public List<DistributionDistance> measureDistancseBetweenConsecutiveWindows(List<QueryWindow> queryWindowList) throws Exception {
		return measureDistancesBetweenPairsOfLaggedWindows(queryWindowList, 1);
	}
	
	public DistributionDistance measureAvgDistanceBetweenConsecutiveWindows(List<QueryWindow> queryWindowList) throws Exception {
		return measureAvgDistanceBetweenPairsOfLaggedWindows(queryWindowList, 1);
	}

	public List<QueryWindow> splitIntoEqualNumberOfQueries(List<Query> all_queries, int windowSizeInNumberOfQueries, boolean sortBasedOnTimestamp) throws Exception {
		List<Query> copied_queries = new ArrayList<Query>(all_queries);
		if (sortBasedOnTimestamp)
			Collections.sort(copied_queries, new QueryTemporalComparator());

		List<QueryWindow> windows = new ArrayList<QueryWindow>();
		List<Query> curWindow = new ArrayList<Query>();
		
		for (Query q: copied_queries) {
			if (curWindow.size() < windowSizeInNumberOfQueries)
				curWindow.add(q.clone());
			else {
				QueryWindow queryWindow = new QueryWindow(curWindow);
				windows.add(queryWindow.clone());
				curWindow.clear();
			}
		}
		if (curWindow.size() > 0) {
			QueryWindow queryWindow = new QueryWindow(curWindow);
			windows.add(queryWindow.clone());
		}
		
		return windows;
	}

	
	public List<QueryWindow> splitIntoTimeEqualWindows(List<Query> all_queries, int windowSizeInDays) throws Exception {
		List<Query> copied_queries = new ArrayList<Query>(all_queries);
		Collections.sort(copied_queries, new QueryTemporalComparator());

		List<QueryWindow>  windows = new ArrayList<QueryWindow>();
		List<Query> curWindow = null;
		Date windowEnd = null;
		Calendar calendar = Calendar.getInstance();
		for (Query q: copied_queries) {
			if (windowEnd == null || q.getTimestamp().after(windowEnd)) {
				if (curWindow!=null) {
					QueryWindow queryWindow = new QueryWindow(curWindow);
					windows.add(queryWindow);
				}
				curWindow = new ArrayList<Query>();
				calendar.setTime(q.getTimestamp());
				calendar.add(Calendar.DATE, windowSizeInDays);
				windowEnd = calendar.getTime();
			}
			curWindow.add(q.clone());
		}
		if (curWindow!=null) {
			QueryWindow queryWindow = new QueryWindow(curWindow);
			windows.add(queryWindow);
		}
		
		return windows;
	}
	
	public void measureWindowSize_AvgConsecutiveDistance(List<Query> all_queries, String whereToSaveFiles) throws Exception {
		PrintStream ps = new PrintStream(whereToSaveFiles);
		String sep = "\t";
		//System.out.println("#WindowSizeInDays"+sep+"avgDistanceBetweenConsecutiveWindows");
		ps.println("#WindowSizeInDays"+sep+"avgDistanceBetweenConsecutiveWindows");
		for (int windowSizeInDays=1; windowSizeInDays<=31; ++windowSizeInDays) {
			List<QueryWindow> windowsLists = splitIntoTimeEqualWindows(all_queries, windowSizeInDays);
			DistributionDistance dist = measureAvgDistanceBetweenConsecutiveWindows(windowsLists);
			if (dist == null)
				break;
			//System.out.println(windowSizeInDays + sep + dist.toString());
			ps.println(windowSizeInDays + sep + dist.toString());
		}
		ps.close();
	}
	
	public void measureWindowSize_Lag_AvgDistance(List<Query> all_queries, String whereToSaveFiles) throws Exception {
		PrintStream ps = new PrintStream(whereToSaveFiles);
		
		String sep = "\t";
		//System.out.println("#WindowSizeInDays"+sep+"lagBetweenPairsOfWindows"+sep+"avgDistanceBetweenPairsOfWindows");
		ps.println("#WindowSizeInDays"+sep+"lagBetweenPairsOfWindows"+sep+"avgDistanceBetweenPairsOfWindows");
		
		for (int windowSizeInDays=7; windowSizeInDays<=30; windowSizeInDays+=7) {
			List<QueryWindow> windows = splitIntoTimeEqualWindows(all_queries, windowSizeInDays);

			for (int lagSize=1; lagSize<windows.size(); ++lagSize) {
				DistributionDistance avgDistance = measureAvgDistanceBetweenPairsOfLaggedWindows(windows, lagSize);
				//System.out.println(windowSizeInDays + sep + lagSize + sep + avgDistance.toString());
				ps.println(windowSizeInDays + sep + lagSize + sep + avgDistance.toString());
			}
		}
		ps.close();
	}

	public void measureWindowSize_WindowId_ConsecutiveDistance(List<Query> all_queries, String whereToSaveFiles) throws Exception {
		PrintStream ps = new PrintStream(whereToSaveFiles);
		
		String sep = "\t";
		//System.out.println("#WindowSizeInDays"+sep+"windowId"+sep+"DistanceBetweenThisAndTheNextWindow");
		ps.println("#WindowSizeInDays"+sep+"windowId"+sep+"DistanceBetweenThisAndTheNextWindow");
			
		for (int windowSizeInDays=7; windowSizeInDays<=30; windowSizeInDays+=7) {
			List<QueryWindow> windows = splitIntoTimeEqualWindows(all_queries, windowSizeInDays);
			List<DistributionDistance> distances = measureDistancseBetweenConsecutiveWindows(windows);
			for (int winId=0; winId<distances.size(); ++winId) {
				//System.out.println(windowSizeInDays + sep + winId + sep + distances.get(winId).toString());
				ps.println(windowSizeInDays + sep + winId + sep + distances.get(winId).toString());
			}
		}
		ps.close();
	}

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
