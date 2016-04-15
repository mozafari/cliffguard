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
        SQLQueryAnalyzer analyzer = new SQLQueryAnalyzer();
        analyzer.setVerbose(true);
        analyzer.analyzeFile(queryFile, schemas);
        Assert.assertFalse(analyzer.hasUnresolvedColumn());
        System.out.println("basic0 parser test passed.");
    }

}
