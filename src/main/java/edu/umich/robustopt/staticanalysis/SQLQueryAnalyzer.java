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

    // TODO: very bad, use another way to config analyzer in future.
    static public class Configuration {
        public String g_mode = "";
        public String p_mode = "";
        public String c_mode = "";
        public String j_mode = "";
        public String a_mode = "";
        public String a_type = "";
        public boolean column_on = true;
        public boolean table_on = true;
        public Configuration() {}
        public Configuration(char mode) {
            a_mode = j_mode = p_mode = g_mode = String.valueOf(mode);
        }
    }
    static public void setConfig(Configuration conf) {
        config = conf;
    }
    static private Configuration config = null;

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
            stats.setQueryGroup(analyzer.getQueryGroups());
            if (!config.g_mode.equals("")) {
                if (config.table_on) stats.printTableGeneralStats(config.g_mode);
                if (config.column_on) stats.printColumnGeneralStats(config.g_mode);
            }
            if (!config.p_mode.equals("")) {
                if (config.table_on) stats.printTableOccurrenceStats(config.p_mode);
                if (config.column_on) stats.printColumnOccurrenceStats(config.p_mode);
            }
            if (!config.c_mode.equals(""))
                if (config.column_on) stats.printCorColumnsStats(config.c_mode);

            if (config.j_mode.equals("j")) {
                stats.printJoinedColumns();
                stats.printJoinInfo();
            }
            if (!config.a_mode.equals("") && !config.a_type.equals("")) {
                stats.printAggregatedColumns(config.a_mode, config.a_type);
                stats.printAggregateInfo();
            }

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

    public List<Pair<String, String>> getSelectColumns() {
        List<Pair<String, String>> res = new ArrayList<>();
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
