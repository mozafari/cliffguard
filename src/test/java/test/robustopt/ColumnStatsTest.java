package test.robustopt;

import edu.umich.robustopt.staticanalysis.SQLQueryAnalyzer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by zhxchen on 4/15/16.
 */
// TODO no dep on analyzer
public class ColumnStatsTest {
    @Test
    public void analyzerBasicTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/basic0_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> station = new HashSet<String>(Arrays.asList("id", "city", "state", "lat_n", "long_w"));
        Set<String> stats = new HashSet<String>(Arrays.asList("id", "month", "temp_f", "rain_i"));
        schemas.put("station", station);
        schemas.put("stats", stats);
        SQLQueryAnalyzer.setConfig(new SQLQueryAnalyzer.Configuration('u'));
        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.setVerbose(true);
        analyzer.analyzeFile(queryFile, schemas);
        Assert.assertFalse(analyzer.hasUnresolvedColumn());
        System.out.println("basic0 stats test passed.");
    }
    @Test
    public void analyzerAdvancedTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/basic1_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> sailors = new HashSet<String>(Arrays.asList("sid", "sname", "rating", "age"));
        Set<String> boats = new HashSet<String>(Arrays.asList("bid", "bname", "color"));
        Set<String> reserves = new HashSet<String>(Arrays.asList("sid", "bid", "rday"));

        schemas.put("Sailors", sailors);
        schemas.put("Boats", boats);
        schemas.put("Reserves", reserves);

        SQLQueryAnalyzer.setConfig(new SQLQueryAnalyzer.Configuration('u'));
        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.setVerbose(true);
        analyzer.analyzeFile(queryFile, schemas);
        Assert.assertFalse(analyzer.hasUnresolvedColumn());
        System.out.println("basic1 stats test passed.");
    }

    @Test
    public void ExtensionTest() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/extension_queries.txt").getFile());

        Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();
        Set<String> sailors = new HashSet<String>(Arrays.asList("sid", "sname", "rating", "age"));
        Set<String> boats = new HashSet<String>(Arrays.asList("bid", "bname", "color"));
        Set<String> reserves = new HashSet<String>(Arrays.asList("sid", "bid", "rday"));

        schemas.put("Sailors", sailors);
        schemas.put("Boats", boats);
        schemas.put("Reserves", reserves);

        SQLQueryAnalyzer.setConfig(new SQLQueryAnalyzer.Configuration('u'));
        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.setVerbose(true);
        analyzer.analyzeFile(queryFile, schemas);
        Assert.assertFalse(analyzer.hasUnresolvedColumn());
        System.out.println("extension stats test passed.");
    }
}
