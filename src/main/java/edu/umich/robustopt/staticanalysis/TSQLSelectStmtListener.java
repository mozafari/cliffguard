package edu.umich.robustopt.staticanalysis;

import javafx.util.Pair;
import edu.umich.robustopt.staticanalysis.Antlr4TSQLAnalyzerParser.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sorrow17 on 2016/2/8.
 */
public class TSQLSelectStmtListener extends Antlr4TSQLAnalyzerBaseListener {

    private enum QueryPart {
        NIL, SELECT, WHERE, FROM, GROUPBY, ORDERBY
    }

    private class ColumnInfo {
        private Pair<String, String> trueName = new Pair<String, String>(null, null);
        private Pair<String, String> queryName;
        private Pair<String, String> resolvedName = new Pair<String, String>(null, null);
        private Pair<String, String> alias = new Pair<String, String>(null, null);
        private Pair<Integer, Integer> pos = new Pair<Integer, Integer>(null, null);
        private boolean resolved = false;

        public ColumnInfo(String t, String c) { queryName = new Pair<String, String>(t, c); }

        public String getQueryTableName() { return queryName.getKey(); }
        public String getQueryColumnName() { return queryName.getValue(); }
        public String getTrueTableName() { return trueName.getKey(); }
        public String getTrueColumnName() { return trueName.getValue(); }
        public String getResolvedTableName() { return resolvedName.getKey(); }
        public String getResolvedColumnName() { return resolvedName.getValue(); }
        public String getTableAlias() { return alias.getKey(); }
        public String getColumnAlias() { return alias.getValue(); }

        public boolean isResolved() { return resolved; }

        public void copyTrueNameFrom(ColumnInfo other) { trueName = other.trueName; }
        public void setTrueName(String tableName, String columnName) { trueName = new Pair<String, String>(tableName, columnName); }
        public void setTableAlias(String alias) { this.alias = new Pair<String, String>(alias, this.alias.getValue()); }
        public void setColumnAlias(String alias) { this.alias = new Pair<String, String>(this.alias.getKey(), alias); }
        public void setResolvedName(String tableName, String columnName) {
            assert !resolved;
            resolvedName = new Pair<String, String>(tableName, columnName);
            resolved = true;
        }
        public void setPosition(Integer line, Integer col) { pos = new Pair<Integer, Integer>(line, col); }

        @Override
        public String toString() {
            return (queryName.getKey()!=null?queryName.getKey()+".":"") +
                    (queryName.getValue()!=null?queryName.getValue():"*") +
                    " -> " + trueName.getKey()+"."+trueName.getValue() +
                    "(" + pos.getKey() + ":" + pos.getValue() + ")";
        }

        @Override
        public boolean equals(Object otherObj) {
            if (otherObj instanceof ColumnInfo) {
                ColumnInfo other = (ColumnInfo) otherObj;
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
        static public boolean matchesColumnName(ColumnInfo column, String columnName) {
            return columnName.equalsIgnoreCase(column.getResolvedColumnName()) ||
                    columnName.equalsIgnoreCase(column.getColumnAlias());
        }
        static public boolean matchesTableName(ColumnInfo column, String tableName) {
            return tableName.equalsIgnoreCase(column.getResolvedTableName()) ||
                    tableName.equalsIgnoreCase(column.getTableAlias());
        }
        static public Pair<String, String> getTrueName(ColumnInfo c) {
            return new Pair<String, String> (c.getTrueTableName(), c.getTrueColumnName());
        }

    }

    class ColumnResolver {
        private ColumnInfo lookUpTrueName(List<ColumnInfo> symbolTable, ColumnInfo c) {
            String currentColumnName = c.getQueryColumnName();

            int countMatches = 0;
            ColumnInfo matchedColumn = null;

            if (c.getQueryTableName()==null) {
                for (ColumnInfo k : symbolTable) {
                    if (Columns.matchesColumnName(k, currentColumnName)) {
                        matchedColumn = k;
                        countMatches++;
                    }
                    if (countMatches>1) return null;
                }
            }
            else {
                String currentTableName = c.getQueryTableName();
                for (ColumnInfo k : symbolTable) {
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
        public void resolveName(ColumnInfo c) {
            if (c.isResolved()) return;
            ColumnInfo matchedColumn;
            Iterator<List<ColumnInfo>> i = TSQLSelectStmtListener.this.symbolStack.descendingIterator();
            while (i.hasNext()) {
                List<ColumnInfo> symbolFrame = i.next();
                matchedColumn = lookUpTrueName(symbolFrame, c);
                if (matchedColumn!=null) {
                    c.copyTrueNameFrom(matchedColumn);
                    c.setResolvedName(null, c.getQueryColumnName());
                    return;
                }
            }
            unresolvedSymbols.add(c);
        }
        public void resolveAll(Collection<ColumnInfo> c) {
            c.stream().forEach(x -> resolveName(x));
        }
    }

    public class QueryInfo {
        private Set<ColumnInfo> select = new HashSet<ColumnInfo>();
        private Set<ColumnInfo> where = new HashSet<ColumnInfo>();
        private Set<ColumnInfo> from = new HashSet<ColumnInfo>();
        private List<ColumnInfo> groupby = new ArrayList<ColumnInfo>();
        private List<ColumnInfo> orderby = new ArrayList<ColumnInfo>();

        public Set<ColumnInfo> getSelectColumnInfo() { return select; }
        public Set<ColumnInfo> getWhereColumnInfo() { return where; }
        public Set<ColumnInfo> getFromColumnInfo() { return from; }
        public List<ColumnInfo> getGroupByColumnInfo() { return groupby; }
        public List<ColumnInfo> getOrderByColumnInfo() { return orderby; }

        public Set<Pair<String, String>> getSelectColumnStrings() { return select.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public Set<Pair<String, String>> getWhereColumnStrings() { return where.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public Set<Pair<String, String>> getFromColumnStrings() { return from.stream().map(Columns::getTrueName).collect(Collectors.toSet()); }
        public List<Pair<String, String>> getGroupByColumnStrings() { return groupby.stream().map(Columns::getTrueName).collect(Collectors.toList()); }
        public List<Pair<String, String>> getOrderByColumnStrings() { return from.stream().map(Columns::getTrueName).collect(Collectors.toList()); }

        public void addSelectColumnInfo(ColumnInfo c) { select.add(c); }
        public void addWhereColumnInfo(ColumnInfo c) { where.add(c); }
        public void addFromColumnInfo(ColumnInfo c) { from.add(c); }
        public void addGroupByColumnInfo(ColumnInfo c) { groupby.add(c); }
        public void addOrderByColumnInfo(ColumnInfo c) { orderby.add(c); }

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
    public void enterTsql_file(Tsql_fileContext ctx) { queryPartStack.push(QueryPart.NIL); }
    @Override
    public void exitTsql_file(Tsql_fileContext ctx) { queryPartStack.pop(); }
    @Override
    public void enterSelect_list(Select_listContext ctx) { queryPartStack.push(QueryPart.SELECT); }
    @Override
    public void exitSelect_list(Select_listContext ctx) { queryPartStack.pop(); }
    @Override
    public void enterQuery_from(Query_fromContext ctx) { queryPartStack.push(QueryPart.FROM); }
    @Override
    public void exitQuery_from(Query_fromContext ctx) { queryPartStack.pop(); }
    @Override
    public void enterQuery_where(Query_whereContext ctx) { queryPartStack.push(QueryPart.WHERE); }
    @Override
    public void exitQuery_where(Query_whereContext ctx) { queryPartStack.pop(); }
    @Override
    public void enterQuery_groupby(Query_groupbyContext ctx) { queryPartStack.push(QueryPart.GROUPBY); }
    @Override
    public void exitQuery_groupby(Query_groupbyContext ctx) { queryPartStack.pop(); }
    @Override
    public void enterOrder_by_clause(Order_by_clauseContext ctx) {
        if (queryPartStack.peek()==QueryPart.SELECT) return;
        queryPartStack.push(QueryPart.ORDERBY);
    }
    @Override
    public void exitOrder_by_clause(Order_by_clauseContext ctx) {
        if (queryPartStack.peek()==QueryPart.SELECT) return;
        queryPartStack.pop();
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
                ColumnInfo column = new ColumnInfo(null, null);
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
        for (ColumnInfo c : lastDerivedSymbols)
            c.setTableAlias(alias);
    }

    @Override
    public void enterQuery_specification(Query_specificationContext ctx) {
        if (nestedLevel==0)
            postOrderQueryGroups.add(new ArrayList<QueryInfo>());

        nestedLevel++;
        swgoStack.push(new QueryInfo());
        symbolStack.push(new ArrayList<ColumnInfo>());
    }

    @Override
    public void enterColumn_alias(Column_aliasContext ctx) { lastUnaliasedColumn.setColumnAlias(ctx.getText()); }

    @Override
    public void exitQuery_specification(Query_specificationContext ctx) {
        nestedLevel--;

        QueryInfo swgoTop = swgoStack.peek();
        swgoStack.pop();

        Set<ColumnInfo> selectInfo = swgoTop.getSelectColumnInfo();
        Set<ColumnInfo> expandedInfo = new HashSet<ColumnInfo>();
        for (ColumnInfo c : selectInfo)
            if (c.getQueryColumnName().equals("*")) {
                if (c.getQueryTableName() == null) {
                    expandedInfo.addAll(symbolStack.peek());
                } else {
                    String tableName = c.getQueryTableName();
                    for (ColumnInfo column : symbolStack.peek())
                        if (Columns.matchesTableName(column, tableName))
                            expandedInfo.add(column);
                }
            }

        selectInfo.removeIf(x->x.getQueryColumnName().equals("*"));
        selectInfo.addAll(expandedInfo);

        ColumnResolver resolver = new ColumnResolver();
        lastDerivedSymbols.clear();
        for (ColumnInfo c : selectInfo) {
            resolver.resolveName(c);
            lastDerivedSymbols.add(c);
        }

        resolver.resolveAll(swgoTop.getFromColumnInfo());
        resolver.resolveAll(swgoTop.getWhereColumnInfo());
        resolver.resolveAll(swgoTop.getGroupByColumnInfo());
        lastResolvedQuery = swgoTop;

        symbolStack.pop();
        if (nestedLevel>0)
            for (ColumnInfo c : selectInfo) {
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
            swgoStack.peek().addSelectColumnInfo(new ColumnInfo(tableName, "*"));
        }
    }

    @Override
    public void enterFull_column_name(Full_column_nameContext ctx) {
        String colName = ctx.column_name().getText().toLowerCase();
        String tableName = ctx.table_name()==null?null:ctx.table_name().getText().toLowerCase();
        QueryInfo target = swgoStack.peek();
        ColumnInfo column = new ColumnInfo(tableName, colName);
        column.setPosition(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
        lastUnaliasedColumn = column;
        switch (queryPartStack.peek()) {
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

    public boolean hasUnresolvedSymbol() { return !unresolvedSymbols.isEmpty(); }

    public Deque<List<QueryInfo>> getQueryGroups() { return postOrderQueryGroups; }

    private Map<String, Set<String>> schemas = new HashMap<String, Set<String>>();

    private Deque<QueryInfo> swgoStack = new ArrayDeque<QueryInfo>();
    private Deque<List<ColumnInfo>> symbolStack = new ArrayDeque<List<ColumnInfo>>();
    private Deque<QueryPart> queryPartStack = new ArrayDeque<QueryPart>();

    private Deque<List<QueryInfo>> postOrderQueryGroups = new ArrayDeque<List<QueryInfo>>();

    private List<ColumnInfo> lastDerivedSymbols = new ArrayList<ColumnInfo>();
    private ColumnInfo lastUnaliasedColumn;
    private QueryInfo lastResolvedQuery;
    private List<ColumnInfo> unresolvedSymbols = new ArrayList<>();

    private int nestedLevel = 0;

    // TODO: tables with schema names?
}
