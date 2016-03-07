package edu.umich.robustopt.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
/**
 * useless class, just so we can run random snippets of code in main()
 * @author stephentu
 *
 */
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.sun.jmx.snmp.tasks.ThreadService;

import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.metering.LatencyMeter;

public class Test {
		
	private static void terminateAllRunningQueries(String dBuser, Statement stmt) throws Exception {
		String sql = "select session_id, statement_id from v_monitor.query_requests where is_executing";
		
		List<Pair<String, Integer>> sesstion_statements = new ArrayList<Pair<String, Integer>>();
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			String sessionId = rs.getString(1);
			Integer statementId = rs.getInt(2);
			sesstion_statements.add(new Pair<String, Integer>(sessionId, statementId));
		}
		rs.close();
		
		System.out.println("There are " + sesstion_statements.size() + " queries running ...");
		// now going to end those queries!
		for (Pair<String, Integer> pair : sesstion_statements) {
			String termSql = "select INTERRUPT_STATEMENT('" + pair.first + "', " + pair.second+")";
			rs = stmt.executeQuery(termSql);
			int rc = 0;
			while (rs.next())
				++rc;
			if (rc!=1)
				throw new Exception("this query didn't work: " + termSql + " as it returned " + rc);
		}
		
	}

	public static void runThingsInTheBackground() {
    	ExecutorService executorService = Executors.newFixedThreadPool(10);

    	executorService.execute(new Runnable() {
    	    public void run() {
    	    	try {
					Thread.sleep(4000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	        System.out.println("Asynchronous task");
    	    }
    	});
        System.out.println("Do you see this first?");		
    	executorService.shutdown();
	}

	public static void runThingsWaitingForThemToFinishWithTimeout() throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Long> future = executor.submit(new Task2());

        try {
            System.out.println("Started..");
            System.out.println(future.get(3, TimeUnit.SECONDS));
            System.out.println("Finished!");
        } catch (TimeoutException e) {
            System.out.println("Terminated!");
        }
              
        executor.shutdownNow();

	}
	
    public static void main(String[] args) {
    	String query = "select  Case when a11.ident_1612 in ('EsE','HqM','Uew','e/M') ";
    	String errMessage = "Invalid input syntax for integer: \"EsE\"";
		System.out.println("errorMessage was<" + errMessage + ">");
		Pattern projectionNamePattern = Pattern.compile("Invalid input syntax for integer: \"([^\"]*)\"");
		Matcher m = projectionNamePattern.matcher(errMessage);
		if (m.find()) {
			String thingToLookFor = "'" + m.group(1) + "'";
			query = query.replaceAll(thingToLookFor, "1234");
			System.out.println("found and replaced as follows: " + query);
		} else {
			System.out.println("Pattern not found!");
		}

    	
    	Double d1 = Double.MAX_VALUE;
    	Double d2 = Double.MIN_VALUE;
    	Double d3 = Double.POSITIVE_INFINITY;
    	Double d4 = Double.NEGATIVE_INFINITY;
    	double dd = d3;
    	double ddd = d4;
    	double dddd = dd + ddd;
    	double n = Double.NaN;
    	double nn = 1.0 + d3;
    	System.out.println(" " + d1 + " " + d2 + " " + d3 + " " + d4 + " " + dd + " " + ddd + " " + Double.isInfinite(d3) + " " + Double.isInfinite(d4) + (d4<0));
    	double sum = 1.0 + Double.POSITIVE_INFINITY + Double.NaN;
    	double mean = sum / 3;
    	System.out.println("sum= " + sum + " " + mean);
    	List<Long> arr = new ArrayList<Long>();
    	arr.add((long) 2); arr.add(Long.MAX_VALUE);
    	System.out.println(MyMathUtils.getMeanLongs(arr) == Long.MAX_VALUE);
    	System.out.println(MyMathUtils.getMaxLongs(arr) == Long.MAX_VALUE);
    	System.out.println(MyMathUtils.getMinLongs(arr));
    	System.out.println(MyMathUtils.getStdLongs(arr) == Long.MAX_VALUE);
    	
	    	try {
		    	//runThingsInTheBackground();
		    	//runThingsWaitingForThemToFinishWithTimeout();
	    		
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	System.out.println("Done.");
	}
}

class Task implements Callable<String> {
    @Override
    public String call() throws Exception {
        System.out.println("Got called!");
        Thread.sleep(4000); // Just to demo a long running task of 4 seconds.
        System.out.println("Woken up!");
        return "Ready!";
    }
}

class Task2 implements Callable<Long> {
    @Override
    public Long call() throws Exception {
        System.out.println("Got called!");
        Thread.sleep(4000); // Just to demo a long running task of 4 seconds.
        System.out.println("Woken up!");
        return 13L;
    }
}


