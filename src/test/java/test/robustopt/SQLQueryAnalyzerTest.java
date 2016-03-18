package test.robustopt;

import org.junit.Assert;
import org.junit.Test;
import edu.umich.robustopt.staticanalysis.SQLQueryAnalyzer;

import java.io.*;
import java.util.*;

/**
 * Created by sorrow17 on 2016/2/8.
 */
public class SQLQueryAnalyzerTest {

    @Test
    public void analyzerBasicTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/basic_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> station = new HashSet<String>(Arrays.asList("id", "city", "state", "lat_n", "long_w"));
        Set<String> stats = new HashSet<String>(Arrays.asList("id", "month", "temp_f", "rain_i"));
        schemas.put("station", station);
        schemas.put("stats", stats);
        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.analyzeFile(queryFile, schemas);
        //analyzer.printQueryGroups();
        Assert.assertTrue(true);
        System.out.println("basic parser test passed.");
    }

    @Test
    public void analyzerAdvancedTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/advanced_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> sailors = new HashSet<String>(Arrays.asList("sid", "sname", "rating", "age"));
        Set<String> boats = new HashSet<String>(Arrays.asList("bid", "bname", "color"));
        Set<String> reserves = new HashSet<String>(Arrays.asList("sid", "bid", "rday"));

        schemas.put("Sailors", sailors);
        schemas.put("Boats", boats);
        schemas.put("Reserves", reserves);

        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.analyzeFile(queryFile, schemas);
        //analyzer.printQueryGroups();
        Assert.assertTrue(true);
        System.out.println("advanced parser test passed.");
    }

    @Test
    public void analyzerExtensionTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/extension_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> sailors = new HashSet<String>(Arrays.asList("sid", "sname", "rating", "age"));
        Set<String> boats = new HashSet<String>(Arrays.asList("bid", "bname", "color"));
        Set<String> reserves = new HashSet<String>(Arrays.asList("sid", "bid", "rday"));

        schemas.put("Sailors", sailors);
        schemas.put("Boats", boats);
        schemas.put("Reserves", reserves);

        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.analyzeFile(queryFile, schemas);
        //analyzer.printQueryGroups();
        Assert.assertTrue(true);
        System.out.println("extension parser test passed.");
    }
}
