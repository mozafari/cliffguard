package edu.umich.robustopt.staticanalysis;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhxchen on 3/17/16.
 */
public class SQLSchemaAnalyzer {
    public void analyzeFile(File queryFile) throws IOException {
        BufferedReader queryReader = new BufferedReader(new FileReader(queryFile));
        analyze(queryReader);
    }

    public void analyzeString(String queryString) throws IOException {
        BufferedReader queryReader = new BufferedReader(new StringReader(queryString));
        analyze(queryReader);
    }

    // TODO: unify schema map and unique list.
    public Map<String, Set<String>> getPlainSchemaMap() {
        return analyzer.getPlainSchemaMap();
    }
    public List<ColumnDescriptor> getUniqueList() { return analyzer.getUniqueList(); }

    private void analyze(Reader queryReader) throws IOException {
        ANTLRInputStream queryStream = new ANTLRInputStream(queryReader);
        Antlr4TSQLAnalyzerLexer lexer = new Antlr4TSQLAnalyzerLexer(queryStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        Antlr4TSQLAnalyzerParser parser = new Antlr4TSQLAnalyzerParser(tokens);

        analyzer = new TSQLDDLStmtListener();
        ParseTreeWalker.DEFAULT.walk(analyzer, parser.tsql_file());
    }

    private TSQLDDLStmtListener analyzer;
}
