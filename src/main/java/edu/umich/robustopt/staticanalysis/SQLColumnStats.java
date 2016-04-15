package edu.umich.robustopt.staticanalysis;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;
import com.jakewharton.fliptables.FlipTable;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.SchemaUtils;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLColumnStats {
    class AprioriFilter {
        public Map<ColumnSet, Integer> getFrequentSet() {
            ColumnSet itemSet = new ColumnSet();
            cols.forEach(itemSet::addAll);

            Set<ColumnSet> origin = new HashSet<>();
            origin.add(new ColumnSet());
            results.add(origin);
            while (!results.get(results.size()-1).isEmpty()) {
                Set<ColumnSet> last = results.get(results.size()-1);
                Set<ColumnSet> current = new HashSet<>();
                for (ColumnSet cs : last)
                    for (ColumnDescriptor column : itemSet.getColumns())
                        if (!cs.contains(column)) {
                            ColumnSet tmp = new ColumnSet();
                            tmp.addAll(cs);
                            tmp.add(column);
                            if (check(tmp)) current.add(tmp);
                        }
                //Map<ColumnSet, Integer> freq = new HashMap<>();
                for (ColumnSet candidate : current)
                    for (ColumnSet tx : cols)
                        if (tx.containsAll(candidate)) freq.put(candidate, freq.getOrDefault(candidate, 0)+1);

                Set<ColumnSet> tail = new HashSet<>();
                for (ColumnSet candidate : current)
                    if (freq.getOrDefault(candidate, 0) >= threshold)
                        tail.add(candidate);
                results.add(tail);
            }

            Map<ColumnSet, Integer> ans = new HashMap<>();
            for (Set<ColumnSet> res : results)
                for (ColumnSet i : res)
                    if (i.size()>0) ans.put(i, freq.get(i));
            return ans;
        }
        public AprioriFilter(List<ColumnSet> c) {
            cols = c;
        }
        private boolean check(ColumnSet cs) {
            Set<ColumnSet> last = results.get(results.size()-1);
            ColumnSet tmp = new ColumnSet();
            tmp.addAll(cs);
            for (ColumnDescriptor i : cs.getColumns()) {
                tmp.remove(i);
                if (!last.contains(tmp)) return false;
                tmp.add(i);
            }
            return true;
        }
        final private List<ColumnSet> cols;
        private List<Set<ColumnSet>> results = new ArrayList<>();
        private Map<ColumnSet, Integer> freq = new HashMap<>();
        private final Integer threshold = 3;
    }

    public void observe(SQLColumnContext ctx) {
        // Current column
        ColumnDescriptor col = ctx.getColumn();

        // Current context
        SQLContext queryCtx = ctx.getCurrentContext();

        // Stats on column frequency
        columnFreq.put(col, columnFreq.getOrDefault(col, 0)+1);

        // Stats on table frequency
        ColumnDescriptor tableCol = new ColumnDescriptor(null, col.getTableName(), null);
        tableFreq.put(tableCol, tableFreq.getOrDefault(tableCol, 0)+1);

        // Stats on # of sql statements
        SQLContext rootCtx = ctx.getContextByLevel(0);
        sqlStmt.add(rootCtx);

        // Stats on table occurrence
        //Set<SQLContext> tableContextSet = tableOccurence.getOrDefault(tableCol, new HashSet<>());
        //tableContextSet.add(rootCtx);
        //tableOccurence.put(tableCol, tableContextSet);

        // Stats on column occurrence
        //Set<SQLContext> columnContextSet = columnOccurence.getOrDefault(col, new HashSet<>());
        //columnContextSet.add(rootCtx);
        //columnOccurence.put(col, columnContextSet);

        ColumnSet s;
        switch (queryCtx.getType()) {
            case SELECT:
                s = selectCorrelatedColumns.getOrDefault(queryCtx, new ColumnSet());
                s.add(col);
                selectCorrelatedColumns.put(queryCtx, s);
                break;
            case EXPRESSION:
                if (ctx.size()>1) {
                    Integer i = 0;
                    SQLContext env = null, last;
                    do {
                        last = env;
                        env = ctx.getContextByLevel(ctx.size()-1-i);
                        i++;
                    } while (env.getType() == SQLContext.ClauseType.EXPRESSION);
                    if (env.getType() == SQLContext.ClauseType.FROM || env.getType() == SQLContext.ClauseType.WHERE) {
                        s = joinedColumns.getOrDefault(last, new ColumnSet());
                        s.add(col);
                        joinedColumns.put(last, s);
                        if (s.size()>=2)
                            joinStats.put(rootCtx, Math.max(s.size(), joinStats.getOrDefault(rootCtx, 0)));
                    }
                    else if (env.getType() == SQLContext.ClauseType.SELECT) {
                        if (queryCtx.getMinorType()!= SQLContext.MinorType.OPERATOR && queryCtx.getMinorType()!= SQLContext.MinorType.NIL) {
                            s = aggregatedColumns.getOrDefault(queryCtx, new ColumnSet());
                            s.add(col);
                            aggregatedColumns.put(queryCtx, s);
                            agOnce.add(rootCtx);
                            agSelect.add(rootCtx);
                            if (queryCtx.getMinorType() == SQLContext.MinorType.EXTREME) {
                                agExtremeOnce.add(rootCtx);
                                agExtremeSelect.add(rootCtx);
                            } else if (queryCtx.getMinorType() == SQLContext.MinorType.TOTAL) {
                                agTotalOnce.add(rootCtx);
                                agTotalSelect.add(rootCtx);
                            }
                        }
                    }
                    else if (env.getType() == SQLContext.ClauseType.GROUPBY) {
                        if (queryCtx.getMinorType()!= SQLContext.MinorType.OPERATOR && queryCtx.getMinorType()!= SQLContext.MinorType.NIL) {
                            s = aggregatedColumns.getOrDefault(queryCtx, new ColumnSet());
                            s.add(col);
                            aggregatedColumns.put(queryCtx, s);
                            agOnce.add(rootCtx);
                            if (queryCtx.getMinorType() == SQLContext.MinorType.EXTREME)
                                agExtremeOnce.add(rootCtx);
                            else if (queryCtx.getMinorType() == SQLContext.MinorType.TOTAL)
                                agTotalOnce.add(rootCtx);
                        }
                    }
                }
                break;
            default:
                break;
        }
    }

    private class Printer {
        public Printer(Writer _w) { writer = new PrintWriter(_w); }
        public Printer() { writer = new PrintWriter(System.out); }
        public void println(String str) {
            writer.println(str);
            writer.flush();
        }
        public void println() {
            writer.println();
            writer.flush();
        }
        public void print(String str) {
            writer.print(str);
            writer.flush();
        }
        public void printPadded(String str, int len) {
            writer.print(str);
            writer.print(String.join("", Collections.nCopies(len-str.length(), " ")));
            writer.flush();
        }

        public int alignLength(List<String> strs) {
            String m = strs.isEmpty()?"++++":strs.stream().max((x,y) -> x.length()-y.length()).get();
            return 4*((m.length()+4)/4);
        }
        private PrintWriter writer;
    }

    private class ColumnSet {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("<");
            String joined = String.join(", ", cols.stream().map(ColumnDescriptor::getTableColumnName).collect(Collectors.toList()));
            sb.append(joined);
            sb.append(">");
            return sb.toString();
        }
        public void add(ColumnDescriptor col) {
            cols.add(col);
        }
        public void addAll(ColumnSet other) { cols.addAll(other.cols); }
        public boolean containsAll(ColumnSet other) { return cols.containsAll(other.cols); }
        public boolean contains(ColumnDescriptor c) { return cols.contains(c); }
        public void remove(ColumnDescriptor c) { cols.remove(c); }

        public Integer size() { return cols.size(); }
        @Override
        public int hashCode() {
            return cols.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ColumnSet) && ((ColumnSet) o).cols.equals(cols);
        }

        public List<ColumnDescriptor> getColumns() { return cols.stream().collect(Collectors.toList()); }

        public ColumnSet() {}
        public ColumnSet(Set<ColumnDescriptor> c) { cols = c; }

        private Set<ColumnDescriptor> cols = new HashSet<>();
    }

    private class StatsTable {
        public void addColumn(String str, String[] bodies) {
            headers.add(str);
            cols.add(bodies);
        }
        public void getTable() {
            FlipTable.of(headers.toArray(new String[headers.size()]),cols.toArray(new String[cols.size()][]));
        }
        private List<String> headers = new ArrayList<>();
        private List<String[]> cols = new ArrayList<>();
    }

    private void printTitle(String title) {
        printer.println();
        printer.print("# ");
        printer.println(title);
        printer.println();
    }

    private String explain(char command) {
        switch (command) {
            case 'u' :
                return "at least once";
            case 's' :
                return "in SELECT clause";
            case 'f' :
                return "in FROM clause";
            case 'w' :
                return "in WHERE clause";
            case 'g' :
                return "in GROUP BY clause";
            case 'o' :
                return "in ORDER BY clause";
            default:
                assert false;
                break;
        }
        return "";
    }

    private ColumnDescriptor colFromPair(Pair<String, String> p) {
        return new ColumnDescriptor(SchemaUtils.defaultSchemaName, p.getKey(), p.getValue());
    }
    private ColumnDescriptor tableFromCol(ColumnDescriptor col) {
        return new ColumnDescriptor(SchemaUtils.defaultSchemaName, col.getTableName(), null);
    }

    private List<ColumnSet> combinationFilter(char command) {
        List<Set<ColumnDescriptor>> res = new ArrayList<>();
        switch (command) {
            case 'u' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i) {
                        res.add(j.getSelectColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                        res.add(j.getFromColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                        res.add(j.getWhereColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                        res.add(j.getGroupByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                        res.add(j.getOrderByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                    }
                }
                break;
            case 's' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.add(j.getSelectColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                }
                break;
            case 'f' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.add(j.getFromColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                }
                break;
            case 'w' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.add(j.getWhereColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                }
                break;
            case 'g' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.add(j.getGroupByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                }
                break;
            case 'o' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.add(j.getOrderByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toSet()));
                }
                break;
            default:
                assert false;
                break;
        }
        return res.stream().map(ColumnSet::new).collect(Collectors.toList());
    }

    private List<List<ColumnDescriptor>> commandFilter(char command) {
        List<List<ColumnDescriptor>> res = new ArrayList<>();
        switch (command) {
            case 'u' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getUnionColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            case 's' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getSelectColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            case 'f' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getFromColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            case 'w' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getWhereColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            case 'g' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getGroupByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            case 'o' :
                for (List<TSQLSelectStmtListener.QueryInfo> i : queryGroup) {
                    res.add(new ArrayList<>());
                    for (TSQLSelectStmtListener.QueryInfo j : i)
                        res.get(res.size() - 1).addAll(j.getOrderByColumnStrings().stream().map(this::colFromPair).collect(Collectors.toList()));
                }
                break;
            default:
                assert false;
                break;
        }
        return res;
    }

    private static <T> void countFreq(Map<T, Integer> m, Collection<T> s) {
        for (T i : s) m.put(i, m.getOrDefault(i, 0)+1);
    }
    private static <T> List<Map.Entry<T, Integer>> sortFreq(Map<T, Integer> freq) {
        return freq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
    }

    public void printTableGeneralStats(char mode) {
        printTitle("General Statistics on TABLES that appear in query file");
        printer.println("Number of unique TABLES appearing in the query file: " + tableFreq.size());
        String ctx;
        if (mode=='u') ctx = "in the query file";
        else ctx = explain(mode);

        printer.println("Number of times a given TABLE has appeared " + ctx + " including multiple occurrences within the same query (rank from the highest to the lowest):");
        List<List<ColumnDescriptor>> m = commandFilter(mode);
        Map<ColumnDescriptor, Integer> freq = new HashMap<>();
        m.forEach(x -> countFreq(freq, x.stream().map(this::tableFromCol).collect(Collectors.toList())));
        List<Map.Entry<ColumnDescriptor, Integer>> l = sortFreq(freq);

        printer.print("- Rank ");
        int len = printer.alignLength(l.stream().map(x -> x.getKey().getTableName()).collect(Collectors.toList()));
        printer.printPadded("Table", len);
        printer.println("Number");
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(String.format("%7s", i.toString() + ". "));

            printer.printPadded(x.getKey().getTableName(), len);
            printer.println(String.valueOf(x.getValue()));
            i++;
        }

        printer.println();
        printer.println("(Total results: " + l.size() + ", listed: " + l.size() + ")");
    }

    public void printColumnGeneralStats(char mode) {
        printTitle("General Statistics on COLUMNS that appear in query file");
        printer.println("Number of unique COLUMNS appearing in the query file: "+ columnFreq.size());
        String ctx;
        if (mode=='u') ctx = "in arbitrary clause";
        else ctx = explain(mode);
        printer.println("Number of times a given COLUMN has appeared " + ctx + " including multiple occurrences within the same query (rank from the highest to the lowest):");
        List<List<ColumnDescriptor>> m = commandFilter(mode);
        Map<ColumnDescriptor, Integer> freq = new HashMap<>();
        m.forEach(x -> countFreq(freq, x));
        List<Map.Entry<ColumnDescriptor, Integer>> l = sortFreq(freq);

        printer.print("- Rank ");
        int len = printer.alignLength(l.stream().map(x -> x.getKey().getTableColumnName()).collect(Collectors.toList()));
        printer.printPadded("Column", len);
        printer.println("Number");
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().getTableColumnName(), len);
            printer.println(String.valueOf(x.getValue()));
            i++;
        }

        printer.println();
        printer.println("(Total results: " + l.size() + ", listed: " + l.size() + ")");
    }

    public void printTableOccurrenceStats(char mode) {
        String ctx = explain(mode);
        printTitle("Popular TABLES in terms of number of queries in which they appear " + ctx + " (rank from the highest number to the lowest): ");
        List<List<ColumnDescriptor>> m = commandFilter(mode);
        Map<ColumnDescriptor, Integer> freq = new HashMap<>();
        m.forEach(x -> countFreq(freq, x.stream().map(this::tableFromCol).collect(Collectors.toSet())));
        List<Map.Entry<ColumnDescriptor, Integer>> l = sortFreq(freq);

        printer.print("- Rank ");
        int len = printer.alignLength(l.stream().map(x -> x.getKey().getTableName()).collect(Collectors.toList()));
        printer.printPadded("Table", len);
        printer.printPadded("Queries", 8);
        printer.println("Percent");
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().getTableName(), len);
            printer.printPadded(String.valueOf(x.getValue()), 8);
            printer.println(x.getValue()*100/sqlStmt.size() + "%");
            i++;
        }

        printer.println();
        printer.println("(Total result(s): " + l.size() + ", " + "listed: " + l.size() + ")");
    }

    public void printColumnOccurrenceStats(char mode) {
        String ctx = explain(mode);
        printTitle("Popular COLUMNS in terms of number of queries in which they appear " + ctx + " (rank from the highest number to the lowest): ");
        List<List<ColumnDescriptor>> m = commandFilter(mode);
        Map<ColumnDescriptor, Integer> freq = new HashMap<>();
        m.forEach(x -> countFreq(freq, x.stream().collect(Collectors.toSet())));
        List<Map.Entry<ColumnDescriptor, Integer>> l = sortFreq(freq);

        printer.print("- Rank ");
        int len = printer.alignLength(l.stream().map(x -> x.getKey().getTableColumnName()).collect(Collectors.toList()));
        printer.printPadded("Column", len);
        printer.printPadded("Queries", 8);
        printer.println("Percent");
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().getTableColumnName(), len);
            printer.printPadded(String.valueOf(x.getValue()), 8);
            printer.println(x.getValue()*100/sqlStmt.size() + "%");
            i++;
        }

        printer.println();
        printer.println("(Total result(s): " + l.size() + ", " + "listed: " + l.size() + ")");
    }

    public void printCorColumnsStats(char mode) {
        String ctx = explain(mode);
        printTitle("popular sets of columns in terms of number of queries in which they appear together " + ctx + ":");
        List<ColumnSet> m = combinationFilter(mode);
        Map<ColumnSet, Integer> freq = new AprioriFilter(m).getFrequentSet();

        List<Map.Entry<ColumnSet, Integer>> l = sortFreq(freq);

        printer.print("- Rank ");
        int len = printer.alignLength(l.stream().map(x->x.getKey().toString()).collect(Collectors.toList()));
        printer.printPadded("Columns", len);
        printer.println("Queries");
        Integer i = 0;
        for (Map.Entry<ColumnSet, Integer> x : l) {
            i++;
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().toString(), len);
            printer.println(String.valueOf(x.getValue()));
        }

        printer.println();
        printer.println("(Total result(s): " + i + ", " + "listed: " + i + ")");
    }

    public void printJoinedColumns() {
        printTitle("Popular COLUMN groups that get joined in queries (rank from the highest to the lowest): ");
        List<ColumnSet> l =
                joinedColumns.entrySet().stream()
                        .map(x -> x.getValue()).collect(Collectors.toList());

        Map<ColumnSet, Integer> freq = new HashMap<>();
        for (ColumnSet x : l)
            freq.put(x, freq.getOrDefault(x, 0)+1);

        List<Map.Entry<ColumnSet, Integer>> res =
                freq.entrySet().stream()
                        .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());

        printer.print("- Rank ");
        int len = printer.alignLength(res.stream().filter(x -> x.getKey().size()>1).map(x->x.getKey().toString()).collect(Collectors.toList()));
        printer.printPadded("Columns", len);
        printer.println("Queries");

        Integer i = 0;
        for (Map.Entry<ColumnSet, Integer> x : res) {
            if (x.getKey().size() <= 1) continue;
            i++;
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().toString(), len);
            printer.println(String.valueOf(x.getValue()));
        }

        printer.println();
        printer.println("(Total result(s): " + i + ", " + "listed: " + i + ")");
    }

    public void printJoinInfo() {
        // TODO: foreign-key joins
        printTitle("General statistics for queries that involve joins");
        printer.println("Number of queries that involve a join: " + joinStats.size());
        printer.println("Number of queries that involve joining exactly two tables: " + joinStats.values().stream().filter(x -> x==2).count());
        printer.println("Number of queries that involve joining three or more tables: " + joinStats.values().stream().filter(x -> x>=3).count());
    }

    public void printAggregatedColumns() {
        printTitle("Popular COLUMNS in terms of aggregate functions in which they appear as parameter (rank from the highest to the lowest)");
        List<ColumnSet> l =
                aggregatedColumns.entrySet().stream()
                        .map(x-> x.getValue()).collect(Collectors.toList());
        Map <ColumnDescriptor, Integer> freq = new HashMap<>();
        for (ColumnSet col : l)
            for (ColumnDescriptor x : col.getColumns())
                freq.put(x, freq.getOrDefault(x, 0)+1);

        List<Map.Entry<ColumnDescriptor, Integer>> res =
                freq.entrySet().stream()
                        .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());

        printer.print("- Rank ");
        int len = printer.alignLength(res.stream().map(x->x.getKey().getTableColumnName()).collect(Collectors.toList()));
        printer.printPadded("Column", len);
        printer.println("Number");

        Integer i = 0;
        for (Map.Entry<ColumnDescriptor, Integer> x : res) {
            i++;
            printer.print(String.format("%7s", i.toString() + ". "));
            printer.printPadded(x.getKey().getTableColumnName(), len);
            printer.println(String.valueOf(x.getValue()));
        }

        printer.println();
        printer.println("(Total result(s): " + i + ", " + "listed: " + i + ")");
    }

    public void printAggregateInfo() {
        // TODO: nested expressions
        // TODO: user-defined functions
        printTitle("General statistics for queries that involve aggregates");
        printer.println("Number of queries that involve at least one aggregate: " + agOnce.size());
        printer.println("Number of queries that involve aggregate columns in their select clause: " + agSelect.size());
        printer.println("Number of queries that involve at least one min/max aggregate: " + agExtremeOnce.size());
        printer.println("Number of queries that involve max/min aggregate in their select clause: " + agExtremeSelect.size());
        printer.println("Number of queries that involve at least one count/avg/sum aggregate: " + agTotalOnce.size());
        printer.println("Number of queries that involve count/avg/sum aggregate in their select clause: " + agTotalSelect.size());
    }

    private Map<ColumnDescriptor, Integer> columnFreq = new HashMap<>();
    private Map<SQLContext, ColumnSet> selectCorrelatedColumns = new HashMap<>();
    private Map<ColumnDescriptor, Integer> tableFreq = new HashMap<>();
    private Map<SQLContext, ColumnSet> joinedColumns = new HashMap<>();
    private Map<SQLContext, ColumnSet> aggregatedColumns = new HashMap<>();
    private Set<SQLContext> sqlStmt = new HashSet<>();

    // TODO: refactor query group
    void setQueryGroup(Deque<List<TSQLSelectStmtListener.QueryInfo>> q) {
        queryGroup = q;
    }

    //private Map<ColumnDescriptor, Set<SQLContext>> tableOccurence = new HashMap<>();
    //private Map<ColumnDescriptor, Set<SQLContext>> columnOccurence = new HashMap<>();
    private Map<SQLContext, Integer> joinStats = new HashMap<>();
    private Deque<List<TSQLSelectStmtListener.QueryInfo>> queryGroup;
    private Printer printer = new Printer();
    private Set<SQLContext> agOnce = new HashSet<>(), agSelect = new HashSet<>(),
            agExtremeOnce = new HashSet<>(), agExtremeSelect = new HashSet<>(),
            agTotalOnce = new HashSet<>(), agTotalSelect = new HashSet<>();
}
