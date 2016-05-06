package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.staticanalysis.SQLColumnContext.SQLColumnContextBuilder;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Created by sorrow17 on 2016/5/6.
 */
public class SQLWalker extends Antlr4TSQLAnalyzerBaseListener {
    @Override
    public void enterSql_clause(Antlr4TSQLAnalyzerParser.Sql_clauseContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.STATEMENT));
    }
    @Override
    public void exitSql_clause(Antlr4TSQLAnalyzerParser.Sql_clauseContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterSelect_list(Antlr4TSQLAnalyzerParser.Select_listContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.SELECT));
    }
    @Override
    public void exitSelect_list(Antlr4TSQLAnalyzerParser.Select_listContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_from(Antlr4TSQLAnalyzerParser.Query_fromContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.FROM));
    }
    @Override
    public void exitQuery_from(Antlr4TSQLAnalyzerParser.Query_fromContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_where(Antlr4TSQLAnalyzerParser.Query_whereContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.WHERE));
    }
    @Override
    public void exitQuery_where(Antlr4TSQLAnalyzerParser.Query_whereContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_groupby(Antlr4TSQLAnalyzerParser.Query_groupbyContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.GROUPBY));
    }
    @Override
    public void exitQuery_groupby(Antlr4TSQLAnalyzerParser.Query_groupbyContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterOrder_by_clause(Antlr4TSQLAnalyzerParser.Order_by_clauseContext ctx) {
        if (SQLContextStack.peek().getType()== SQLContext.ClauseType.SELECT) return;
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.ORDERBY));
    }

    @Override
    public void enterAggregate_windowed_function(Antlr4TSQLAnalyzerParser.Aggregate_windowed_functionContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.EXPRESSION));
    }
    @Override
    public void exitAggregate_windowed_function(Antlr4TSQLAnalyzerParser.Aggregate_windowed_functionContext ctx) {
        SQLContextStack.pop();
    }

    @Override
    public void enterBinary_operator_expression(Antlr4TSQLAnalyzerParser.Binary_operator_expressionContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.EXPRESSION));
    }
    @Override
    public void exitBinary_operator_expression(Antlr4TSQLAnalyzerParser.Binary_operator_expressionContext ctx) {
        SQLContextStack.pop();
    }

    @Override
    public void exitOrder_by_clause(Antlr4TSQLAnalyzerParser.Order_by_clauseContext ctx) {
        if (SQLContextStack.peek().getType() == SQLContext.ClauseType.SELECT) return;
        SQLContextStack.pop();
    }
    @Override
    public void enterSelect_list_elem(Antlr4TSQLAnalyzerParser.Select_list_elemContext ctx) {
        if (ctx.select_list_elem_star()!=null) {
            currentPos = new Pair<>(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
            List<ColumnDescriptor> l = meta.get(currentPos);
            l.forEach(this::doStats);
        }
    }

    @Override
    public void enterFull_column_name(Antlr4TSQLAnalyzerParser.Full_column_nameContext ctx) {
        currentPos = new Pair<>(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
        List<ColumnDescriptor> l = meta.get(currentPos);
        l.forEach(this::doStats);
    }

    private void doStats(ColumnDescriptor col) {
        SQLColumnContextBuilder sccb = new SQLColumnContextBuilder();
        sccb.setColumnName(col);
        sccb.setPosition(currentPos);
        sccb.setContextStack((ArrayDeque<SQLContext>) SQLContextStack);
        statsObserver.observe(sccb.build());
    }

    public SQLWalker(Map<Pair<Integer, Integer>, List<ColumnDescriptor>> m) { meta = m; }
    private Deque<SQLContext> SQLContextStack = new ArrayDeque<>();
    private SQLColumnStats statsObserver;
    private Pair<Integer, Integer> currentPos;
    public void setStats(SQLColumnStats s) {
        statsObserver = s;
    }

    private Map<Pair<Integer, Integer>, List<ColumnDescriptor>> meta;
}
