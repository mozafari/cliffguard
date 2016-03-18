package test.robustopt;

import edu.umich.robustopt.staticanalysis.SQLSchemaAnalyzer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by sorrow17 on 2016/3/18.
 */
public class SQLSchemaAnalyzerTest {

    @Test
    public void analyzerBasic0Test() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/basic0_schema.txt").getFile());

        analyzer = new SQLSchemaAnalyzer();
        analyzer.analyzeFile(queryFile);
        System.out.println(analyzer.getPlainSchemaMap());
        System.out.println("basic0 schema parser test passed.");
    }

    @Test
    public void analyzerBasic1Test() throws IOException {
        ClassLoader cl = this.getClass().getClassLoader();
        File queryFile = new File(cl.getResource("ParserTest/basic1_schema.txt").getFile());

        analyzer = new SQLSchemaAnalyzer();
        analyzer.analyzeFile(queryFile);
        System.out.println(analyzer.getPlainSchemaMap());
        System.out.println("basic1 schema parser test passed.");
    }

    private SQLSchemaAnalyzer analyzer;
}
