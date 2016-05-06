package edu.umich.robustopt.staticanalysis;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;
import com.jakewharton.fliptables.FlipTable;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLColumnStats {
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

        // Stats on # of sql statments
        SQLContext rootCtx = ctx.getContextByLevel(0);
        sqlStmt.add(rootCtx);

        // Stats on table occurrence
        Set<SQLContext> tableContextSet = tableOccurence.getOrDefault(tableCol, new HashSet<>());
        tableContextSet.add(rootCtx);
        tableOccurence.put(tableCol, tableContextSet);

        // Stats on column occurence
        Set<SQLContext> columnContextSet = columnOccurence.getOrDefault(col, new HashSet<>());
        columnContextSet.add(rootCtx);
        columnOccurence.put(col, columnContextSet);

        ColumnSet s;
        switch (queryCtx.getType()) {
            case SELECT:
                s = selectCorrelatedColumns.getOrDefault(queryCtx, new ColumnSet());
                s.add(col);
                selectCorrelatedColumns.put(queryCtx, s);
                break;
            case EXPRESSION:
                if (ctx.size()>1) {
                    SQLContext env = ctx.getContextByLevel(ctx.size()-2);
                    if (env.getType() == SQLContext.ClauseType.FROM || env.getType() == SQLContext.ClauseType.WHERE) {
                        s = joinedColumns.getOrDefault(queryCtx, new ColumnSet());
                        s.add(col);
                        joinedColumns.put(queryCtx, s);
                    }
                    else if (env.getType() == SQLContext.ClauseType.SELECT) {
                        s = aggregatedColumns.getOrDefault(queryCtx, new ColumnSet());
                        s.add(col);
                        aggregatedColumns.put(queryCtx, s);
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

        @Override
        public int hashCode() {
            return cols.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ColumnSet) && ((ColumnSet) o).cols.equals(cols);
        }

        public List<ColumnDescriptor> getColumns() { return cols.stream().collect(Collectors.toList()); }

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
        printer.print("* ");
        printer.println(title);
        printer.println();
    }

    public void printGeneralStats() {
        printTitle("[General Stats]");
        printer.println("The number of queried tables: " + tableFreq.size());
        printer.println("Queried tables: ");
        printer.println(String.join(", ", tableFreq.keySet().stream().map(x->x.getTableName().toUpperCase()).collect(Collectors.toList())));
        printer.println();
        printer.println("The number of queried columns: "+ columnFreq.size());
        printer.println("Queried columns: ");
        printer.println(String.join(", ", columnFreq.keySet().stream().map(ColumnDescriptor::getTableColumnName).collect(Collectors.toList())));
        printer.println();
        printer.println("Totally " + sqlStmt.size() + " SQL statements queried.");
    }

    public void printTableOccurenceStats() {
        printTitle("[Table Occurrence Stats]");
        List<Map.Entry<ColumnDescriptor, Set<SQLContext>>> l =
                tableOccurence.entrySet().stream()
                        .sorted((x,y)->y.getValue().size()-x.getValue().size()).collect(Collectors.toList());
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Set<SQLContext>> x : l) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().getTableName() +
                    ": " +
                    x.getValue().size() +
                    " (" + x.getValue().size()*100/sqlStmt.size() + "%)");
            i++;
        }
    }

    public void printColumnOccurenceStats() {
        printTitle("[Column Occurence Stats]");
        List<Map.Entry<ColumnDescriptor, Set<SQLContext>>> l =
                columnOccurence.entrySet().stream()
                        .sorted((x,y)->y.getValue().size()-x.getValue().size()).collect(Collectors.toList());
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Set<SQLContext>> x : l) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().getTableColumnName() +
                    ": " +
                    x.getValue().size() +
                    " (" + x.getValue().size()*100/sqlStmt.size() + "%)");

            i++;
        }
    }

    public void printCorColumnsStats() {
        printTitle("[Correlated Columns Stats]");
        Map<ColumnSet, Integer> tupleFreq = new HashMap<>();
        selectCorrelatedColumns.entrySet().stream()
                .map(Map.Entry::getValue)
                        .forEach(x->tupleFreq.put(x,tupleFreq.getOrDefault(x,0)+1));

        List<Map.Entry<ColumnSet, Integer>> l =
        tupleFreq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());

        Integer i = 1;
        for (Map.Entry<ColumnSet, Integer> x : l) {
            if (x.getKey().cols.size()>1) {
                printer.print(i.toString() + ". ");
                printer.println(x.getKey() + ": " + x.getValue());
                i++;
            }
        }
    }

    public void printFreqStats() {
        printTitle("[Column Frequencies Stats]");
        List<Map.Entry<ColumnDescriptor, Integer>> l =
                columnFreq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().getTableName()+"."+x.getKey().getColumnName()+": "+x.getValue());
            i++;
        }
    }

    public void printTableFreq() {
        printTitle("[Table Frequencies Stats]");
        List<Map.Entry<ColumnDescriptor, Integer>> l =
                tableFreq.entrySet().stream()
                        .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : l) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().getTableName()+": "+x.getValue());
            i++;
        }
    }

    public void printJoinedColumns() {
        printTitle("[Joined Columns Stats]");
        List<ColumnSet> l =
                joinedColumns.entrySet().stream()
                        .map(x -> x.getValue()).collect(Collectors.toList());

        Map<ColumnSet, Integer> freq = new HashMap<>();
        for (ColumnSet x : l)
            freq.put(x, freq.getOrDefault(x, 0)+1);

        List<Map.Entry<ColumnSet, Integer>> res =
                freq.entrySet().stream()
                        .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());

        Integer i = 1;
        for (Map.Entry<ColumnSet, Integer> x : res) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().toString() + ": " + x.getValue() + " (times)");
            i++;
        }
    }

    public void printAggregatedColumns() {
        printTitle("[Aggregated Columns Stats]");
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

        Integer i = 1;
        for (Map.Entry<ColumnDescriptor, Integer> x : res) {
            printer.print(i.toString() + ". ");
            printer.println(x.getKey().getTableColumnName() + ": " + x.getValue() + " (times)");
            i++;
        }
    }
    private Map<ColumnDescriptor, Integer> columnFreq = new HashMap<>();
    private Map<SQLContext, ColumnSet> selectCorrelatedColumns = new HashMap<>();
    private Map<ColumnDescriptor, Integer> tableFreq = new HashMap<>();
    private Map<SQLContext, ColumnSet> joinedColumns = new HashMap<>();
    private Map<SQLContext, ColumnSet> aggregatedColumns = new HashMap<>();
    private Set<SQLContext> sqlStmt = new HashSet<>();

    private Map<ColumnDescriptor, Set<SQLContext>> tableOccurence = new HashMap<>();
    private Map<ColumnDescriptor, Set<SQLContext>> columnOccurence = new HashMap<>();
    private Printer printer = new Printer();
}
