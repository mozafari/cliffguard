package edu.umich.robustopt.staticanalysis;

import javafx.util.Pair;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import edu.umich.robustopt.staticanalysis.TSQLSelectStmtListener.QueryInfo;

import java.io.*;
import java.util.*;

/**
 * Created by sorrow17 on 2016/2/8.
 */
public class SQLQueryAnalyzer {

    public void analyzeFile(File queryFile, Map<String, Set<String>> schemas) throws IOException {
        BufferedReader queryReader = new BufferedReader(new FileReader(queryFile));
        analyze(queryReader, schemas);
    }

    public void analyzeString(String queryString, Map<String, Set<String>> schemas) throws IOException {
        BufferedReader queryReader = new BufferedReader(new StringReader(queryString));
        analyze(queryReader, schemas);
    }

    private void analyze(Reader queryReader, Map<String, Set<String>> schemas) throws IOException {
        ANTLRInputStream queryStream = new ANTLRInputStream(queryReader);
        Antlr4TSQLAnalyzerLexer lexer = new Antlr4TSQLAnalyzerLexer(queryStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Antlr4TSQLAnalyzerParser parser = new Antlr4TSQLAnalyzerParser(tokens);

        analyzer = new TSQLSelectStmtListener();
        analyzer.setSchemas(schemas);
        ParseTreeWalker.DEFAULT.walk(analyzer, parser.tsql_file());
    }


    public void printQueryGroups() {
        System.out.println(">>>========================================");
        analyzer.getQueryGroups().stream().forEach(System.out::println);
        System.out.println(">>>========================================");
    }

    public Set<Pair<String, String>> getSelectStats() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getSelectColumnStrings()));
        return res;
    }

    public Set<Pair<String, String>> getFromStats() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getFromColumnStrings()));
        return res;
    }

    public Set<Pair<String, String>> getWhereStats() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getWhereColumnStrings()));
        return res;
    }

    public List<Pair<String, String>> getGroupByStats() {
        List<Pair<String, String>> res = new ArrayList<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getGroupByColumnStrings()));
        return res;
    }

    public List<Pair<String, String>> getOrderByStats() {
        List<Pair<String, String>> res = new ArrayList<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getOrderByColumnStrings()));
        return res;
    }

    private TSQLSelectStmtListener analyzer;
}
