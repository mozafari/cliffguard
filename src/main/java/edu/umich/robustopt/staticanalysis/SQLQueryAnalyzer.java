package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;
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
        BufferedReader queryReader0 = new BufferedReader(new FileReader(queryFile));
        analyze(queryReader0, schemas);
        BufferedReader queryReader1 = new BufferedReader(new FileReader(queryFile));
        doStats(queryReader1);
    }

    public void analyzeString(String queryString, Map<String, Set<String>> schemas) throws IOException {
        BufferedReader queryReader0 = new BufferedReader(new StringReader(queryString));
        analyze(queryReader0, schemas);
        BufferedReader queryReader1 = new BufferedReader(new StringReader(queryString));
        doStats(queryReader1);
    }

    private void doStats(Reader queryReader) throws IOException {
        ANTLRInputStream queryStream = new ANTLRInputStream(queryReader);
        Antlr4TSQLAnalyzerLexer lexer = new Antlr4TSQLAnalyzerLexer(queryStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Antlr4TSQLAnalyzerParser parser = new Antlr4TSQLAnalyzerParser(tokens);

        SQLWalker walker = new SQLWalker(analyzer.getRawResult());
        walker.setStats(stats);
        ParseTreeWalker.DEFAULT.walk(walker, parser.tsql_file());

        if (verbose) {
            stats.printGeneralStats();
            stats.printFreqStats();
            stats.printTableFreq();
            stats.printColumnOccurenceStats();
            stats.printTableOccurenceStats();
            stats.printCorColumnsStats();
            stats.printJoinedColumns();
            stats.printAggregatedColumns();
            System.out.println();
        }
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

    public Set<Pair<String, String>> getSelectColumns() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getSelectColumnStrings()));
        return res;
    }

    public Set<Pair<String, String>> getFromColumns() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getFromColumnStrings()));
        return res;
    }

    public Set<Pair<String, String>> getWhereColumns() {
        HashSet<Pair<String, String>> res = new HashSet<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getWhereColumnStrings()));
        return res;
    }

    public List<Pair<String, String>> getGroupByColumns() {
        List<Pair<String, String>> res = new ArrayList<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getGroupByColumnStrings()));
        return res;
    }

    public List<Pair<String, String>> getOrderByColumns() {
        List<Pair<String, String>> res = new ArrayList<Pair<String, String>>();
        for (List<QueryInfo> q : analyzer.getQueryGroups())
            q.forEach(x -> res.addAll(x.getOrderByColumnStrings()));
        return res;
    }

    public SQLQueryAnalyzer() { stats = new SQLColumnStats(); }
    public SQLQueryAnalyzer(SQLColumnStats s) { stats = s; }

    public boolean hasUnresolvedColumn() { return analyzer.hasUnresolvedSymbol(); }
    public void setVerbose(boolean v) { verbose = v; }
    private boolean verbose = false;
    private TSQLSelectStmtListener analyzer;
    private SQLColumnStats stats;
}
