package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.staticanalysis.Antlr4TSQLAnalyzerParser.*;

import java.util.*;
import java.util.stream.Collectors;
import edu.umich.robustopt.staticanalysis.SQLColumnContext.SQLColumnContextBuilder;

/**
 * Created by sorrow17 on 2016/2/8.
 */
class TSQLSelectStmtListener extends Antlr4TSQLAnalyzerBaseListener {

    class ColumnNameInfo {
        private Pair<String, String> trueName = new Pair<>(null, null);
        private Pair<String, String> queryName;
        private Pair<String, String> resolvedName = new Pair<>(null, null);
        private Pair<String, String> alias = new Pair<>(null, null);
        private Pair<Integer, Integer> pos = new Pair<>(null, null);
        private boolean resolved = false;

        public ColumnNameInfo(String t, String c) { queryName = new Pair<>(t, c); }

        public String getQueryTableName() { return queryName.getKey(); }
        public String getQueryColumnName() { return queryName.getValue(); }
        public String getTrueTableName() { return trueName.getKey(); }
        public String getTrueColumnName() { return trueName.getValue(); }
        public String getResolvedTableName() { return resolvedName.getKey(); }
        public String getResolvedColumnName() { return resolvedName.getValue(); }
        public String getTableAlias() { return alias.getKey(); }
        public String getColumnAlias() { return alias.getValue(); }
        public Pair<Integer, Integer> getPosition() { return pos; }

        public boolean isResolved() { return resolved; }

        public void copyTrueNameFrom(ColumnNameInfo other) { trueName = other.trueName; }
        public void setTrueName(String tableName, String columnName) { trueName = new Pair<>(tableName, columnName); }
        public void setTableAlias(String alias) { this.alias = new Pair<>(alias, this.alias.getValue()); }
        public void setColumnAlias(String alias) { this.alias = new Pair<>(this.alias.getKey(), alias); }
        public void setResolvedName(String tableName, String columnName) {
            assert !resolved;
            resolvedName = new Pair<>(tableName, columnName);
            resolved = true;
        }
        public void setPosition(Integer line, Integer col) { pos = new Pair<>(line, col); }

        @Override
        public String toString() {
            return (queryName.getKey()!=null?queryName.getKey()+".":"") +
                    (queryName.getValue()!=null?queryName.getValue():"*") +
                    " -> " + trueName.getKey()+"."+trueName.getValue() +
                    "(" + pos.getKey() + ":" + pos.getValue() + ")";
        }

        @Override
        public boolean equals(Object otherObj) {
            if (otherObj instanceof ColumnNameInfo) {
                ColumnNameInfo other = (ColumnNameInfo) otherObj;
                return Objects.equals(other.getColumnAlias(), this.getColumnAlias()) &&
                        Objects.equals(other.getTableAlias(), this.getTableAlias()) &&
                        Objects.equals(other.getResolvedColumnName(), this.getResolvedColumnName()) &&
                        Objects.equals(other.getResolvedTableName(), this.getResolvedTableName()) &&
                        Objects.equals(other.getQueryColumnName(), this.getQueryColumnName()) &&
                        Objects.equals(other.getQueryTableName(), this.getQueryTableName()) &&
                        Objects.equals(other.getTrueColumnName(), this.getTrueColumnName()) &&
                        Objects.equals(other.getTrueTableName(), this.getTrueTableName());
            }
            return false;
        }
    }

    static private class Columns {
        static public boolean matchesColumnName(ColumnNameInfo column, String columnName) {
            return columnName.equalsIgnoreCase(column.getResolvedColumnName()) ||
                    columnName.equalsIgnoreCase(column.getColumnAlias());
        }
        static public boolean matchesTableName(ColumnNameInfo column, String tableName) {
            return tableName.equalsIgnoreCase(column.getResolvedTableName()) ||
                    tableName.equalsIgnoreCase(column.getTableAlias());
        }
        static public Pair<String, String> getTrueName(ColumnNameInfo c) {
            return new Pair<>(c.getTrueTableName(), c.getTrueColumnName());
        }

    }

    private class ColumnResolver {
        private ColumnNameInfo lookUpTrueName(List<ColumnNameInfo> symbolTable, ColumnNameInfo c) {
            String currentColumnName = c.getQueryColumnName();

            int countMatches = 0;
            ColumnNameInfo matchedColumn = null;

            if (c.getQueryTableName()==null) {
                for (ColumnNameInfo k : symbolTable) {
                    if (Columns.matchesColumnName(k, currentColumnName)) {
                        matchedColumn = k;
                        countMatches++;
                    }
                    if (countMatches>1) return null;
                }
            }
            else {
                String currentTableName = c.getQueryTableName();
                for (ColumnNameInfo k : symbolTable) {
                    if (Columns.matchesColumnName(k, currentColumnName)
                            && Columns.matchesTableName(k, currentTableName)) {
                        matchedColumn = k;
                        countMatches++;
                    }
                    if (countMatches==2)
                        System.out.println(c.toString());
                    if (countMatches>1) return null;
                }
            }
            return matchedColumn;
        }
        public void resolveName(ColumnNameInfo c) {
            if (c.isResolved()) return;
            ColumnNameInfo matchedColumn;
            Iterator<List<ColumnNameInfo>> i = TSQLSelectStmtListener.this.symbolStack.descendingIterator();
            while (i.hasNext()) {
                List<ColumnNameInfo> symbolFrame = i.next();
                matchedColumn = lookUpTrueName(symbolFrame, c);
                if (matchedColumn!=null) {
                    c.copyTrueNameFrom(matchedColumn);
                    c.setResolvedName(null, c.getQueryColumnName());

                    SQLColumnContextBuilder sccb = new SQLColumnContextBuilder();
                    sccb.setColumnNameInfo(c);
                    sccb.setContextStack((ArrayDeque<SQLContext>) SQLContextStack);
                    statsObserver.observe(sccb.build());
                    return;
                }
            }
            unresolvedSymbols.add(c);
        }
        public void resolveAll(Collection<ColumnNameInfo> c) { c.stream().forEach(this::resolveName); }
    }

    public class QueryInfo {
        private Set<ColumnNameInfo> select = new HashSet<>();
        private Set<ColumnNameInfo> where = new HashSet<>();
        private Set<ColumnNameInfo> from = new HashSet<>();
        private List<ColumnNameInfo> groupby = new ArrayList<>();
        private List<ColumnNameInfo> orderby = new ArrayList<>();

        public Set<ColumnNameInfo> getSelectColumnInfo() { return select; }
        public Set<ColumnNameInfo> getWhereColumnInfo() { return where; }
        public Set<ColumnNameInfo> getFromColumnInfo() { return from; }
        public List<ColumnNameInfo> getGroupByColumnInfo() { return groupby; }
        public List<ColumnNameInfo> getOrderByColumnInfo() { return orderby; }

        public Set<Pair<String, String>> getSelectColumnStrings() { return select.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public Set<Pair<String, String>> getWhereColumnStrings() { return where.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public Set<Pair<String, String>> getFromColumnStrings() { return from.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public List<Pair<String, String>> getGroupByColumnStrings() { return groupby.stream().map(Columns::getTrueName).collect(Collectors.toList()); }
        public List<Pair<String, String>> getOrderByColumnStrings() { return from.stream().map(Columns::getTrueName).collect(Collectors.toList()); }

        public void addSelectColumnInfo(ColumnNameInfo c) { select.add(c); }
        public void addWhereColumnInfo(ColumnNameInfo c) { where.add(c); }
        public void addFromColumnInfo(ColumnNameInfo c) { from.add(c); }
        public void addGroupByColumnInfo(ColumnNameInfo c) { groupby.add(c); }
        public void addOrderByColumnInfo(ColumnNameInfo c) { orderby.add(c); }

        @Override
        public String toString() {
            return "{select: " + select +
                    ",\n where: " + where +
                    ",\n from: " + from +
                    ",\n groupby: " + groupby +
                    ",\n orderby: " + orderby + "}";
        }
    }

    @Override
    public void enterTsql_file(Tsql_fileContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.NIL));
    }
    @Override
    public void exitTsql_file(Tsql_fileContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterSelect_list(Select_listContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.SELECT));
    }
    @Override
    public void exitSelect_list(Select_listContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_from(Query_fromContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.FROM));
    }
    @Override
    public void exitQuery_from(Query_fromContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_where(Query_whereContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.WHERE));
    }
    @Override
    public void exitQuery_where(Query_whereContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterQuery_groupby(Query_groupbyContext ctx) {
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.GROUPBY));
    }
    @Override
    public void exitQuery_groupby(Query_groupbyContext ctx) { SQLContextStack.pop(); }
    @Override
    public void enterOrder_by_clause(Order_by_clauseContext ctx) {
        if (SQLContextStack.peek().getType()== SQLContext.ClauseType.SELECT) return;
        SQLContextStack.push(new SQLContext(SQLContext.ClauseType.ORDERBY));
    }
    @Override
    public void exitOrder_by_clause(Order_by_clauseContext ctx) {
        if (SQLContextStack.peek().getType() == SQLContext.ClauseType.SELECT) return;
        SQLContextStack.pop();
        symbolStack.push(lastDerivedSymbols);
        symbolStack.peek().stream().forEach(x->x.setTableAlias(x.getQueryTableName()));
        new ColumnResolver().resolveAll(lastResolvedQuery.getOrderByColumnInfo());
        symbolStack.pop();
    }

    @Override
    public void enterTable_source_item(Table_source_itemContext ctx) {
        if (ctx.table_name_with_hint()!=null) {
            String tableName = ctx.table_name_with_hint().table_name().getText().toLowerCase();
            Set<String> tableColumns = schemas.get(tableName);
            lastDerivedSymbols.clear();
            for (String columnName : tableColumns) {
                ColumnNameInfo column = new ColumnNameInfo(null, null);
                column.setTrueName(tableName, columnName);
                column.setResolvedName(tableName, columnName);
                column.setPosition(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
                symbolStack.peek().add(column);
                lastDerivedSymbols.add(column);
            }
        }
    }

    @Override
    public void enterTable_alias(Table_aliasContext ctx) {
        String alias = ctx.getText();
        for (ColumnNameInfo c : lastDerivedSymbols)
            c.setTableAlias(alias);
    }

    @Override
    public void enterQuery_specification(Query_specificationContext ctx) {
        if (nestedLevel==0)
            postOrderQueryGroups.add(new ArrayList<>());

        nestedLevel++;
        swgoStack.push(new QueryInfo());
        symbolStack.push(new ArrayList<>());
    }

    @Override
    public void enterColumn_alias(Column_aliasContext ctx) { lastUnaliasedColumn.setColumnAlias(ctx.getText()); }

    @Override
    public void exitQuery_specification(Query_specificationContext ctx) {
        nestedLevel--;

        QueryInfo swgoTop = swgoStack.peek();
        swgoStack.pop();

        Set<ColumnNameInfo> selectInfo = swgoTop.getSelectColumnInfo();
        Set<ColumnNameInfo> expandedInfo = new HashSet<>();
        for (ColumnNameInfo c : selectInfo)
            if (c.getQueryColumnName().equals("*")) {
                if (c.getQueryTableName() == null) {
                    expandedInfo.addAll(symbolStack.peek());
                } else {
                    String tableName = c.getQueryTableName();
                    for (ColumnNameInfo column : symbolStack.peek())
                        if (Columns.matchesTableName(column, tableName))
                            expandedInfo.add(column);
                }
            }

        selectInfo.removeIf(x->x.getQueryColumnName().equals("*"));
        selectInfo.addAll(expandedInfo);

        ColumnResolver resolver = new ColumnResolver();
        lastDerivedSymbols.clear();
        for (ColumnNameInfo c : selectInfo) {
            resolver.resolveName(c);
            lastDerivedSymbols.add(c);
        }

        resolver.resolveAll(swgoTop.getFromColumnInfo());
        resolver.resolveAll(swgoTop.getWhereColumnInfo());
        resolver.resolveAll(swgoTop.getGroupByColumnInfo());
        lastResolvedQuery = swgoTop;

        symbolStack.pop();
        if (nestedLevel>0)
            for (ColumnNameInfo c : selectInfo) {
                int idx = symbolStack.peek().indexOf(c);
                if (idx == -1)
                    symbolStack.peek().add(c);
                else
                    symbolStack.peek().set(idx, c);
            }

        postOrderQueryGroups.getLast().add(swgoTop);
    }

    @Override
    public void enterSelect_list_elem(Select_list_elemContext ctx) {
        if (ctx.select_list_elem_star()!=null) {
            String tableName = ctx.table_name()==null?null:ctx.table_name().getText().toLowerCase();
            swgoStack.peek().addSelectColumnInfo(new ColumnNameInfo(tableName, "*"));
        }
    }

    @Override
    public void enterFull_column_name(Full_column_nameContext ctx) {
        String colName = ctx.column_name().getText().toLowerCase();
        String tableName = ctx.table_name()==null?null:ctx.table_name().getText().toLowerCase();
        QueryInfo target = swgoStack.peek();
        ColumnNameInfo column = new ColumnNameInfo(tableName, colName);
        column.setPosition(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
        lastUnaliasedColumn = column;
        switch (SQLContextStack.peek().getType()) {
            case SELECT:
                target.addSelectColumnInfo(column);
                break;
            case WHERE:
                target.addWhereColumnInfo(column);
                break;
            case FROM:
                target.addFromColumnInfo(column);
                break;
            case GROUPBY:
                target.addGroupByColumnInfo(column);
                break;
            case ORDERBY:
                lastResolvedQuery.addOrderByColumnInfo(column);
                break;
            default:
                assert false;
        }
    }

    public void setSchemas(Map<String, Set<String>> schemas) {
        for (String i : schemas.keySet())
            this.schemas.put(i.toLowerCase(), schemas.get(i));
    }
    public void setStats(SQLColumnStats s) {
        statsObserver = s;
    }

    public boolean hasUnresolvedSymbol() { return !unresolvedSymbols.isEmpty(); }

    public Deque<List<QueryInfo>> getQueryGroups() { return postOrderQueryGroups; }

    private Map<String, Set<String>> schemas = new HashMap<>();

    private Deque<QueryInfo> swgoStack = new ArrayDeque<>();
    private Deque<List<ColumnNameInfo>> symbolStack = new ArrayDeque<>();
    private Deque<SQLContext> SQLContextStack = new ArrayDeque<>();

    private Deque<List<QueryInfo>> postOrderQueryGroups = new ArrayDeque<>();

    private List<ColumnNameInfo> lastDerivedSymbols = new ArrayList<>();
    private ColumnNameInfo lastUnaliasedColumn;
    private QueryInfo lastResolvedQuery;
    private List<ColumnNameInfo> unresolvedSymbols = new ArrayList<>();

    private int nestedLevel = 0;
    private SQLColumnStats statsObserver;
    // TODO: tables with schema names?
}
