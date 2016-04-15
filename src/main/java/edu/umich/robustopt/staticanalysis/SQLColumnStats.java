package edu.umich.robustopt.staticanalysis;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLColumnStats {
    public void observe(SQLColumnContext ctx) {
        ColumnDescriptor col = ctx.getColumn();
        freq.put(col, freq.getOrDefault(col, 0)+1);
        ColumnDescriptor tableCol = new ColumnDescriptor(null, col.getTableName(), null);
        tableFreq.put(tableCol, tableFreq.getOrDefault(tableCol, 0)+1);
        SQLContext queryCtx = ctx.getCurrentContext();
        switch (queryCtx.getType()) {
            case SELECT:
                Set<ColumnDescriptor> s = selectTuples.getOrDefault(queryCtx, new HashSet<>());
                s.add(col);
                selectTuples.put(queryCtx, s);
                break;
            default:
                break;
        }
    }

    public void printTupleStats() {
        System.out.println("[Tuple Stats]==============================");
        Map<Set<ColumnDescriptor>, Integer> tupleFreq = new HashMap<>();
        selectTuples.entrySet().stream()
                .map(Map.Entry::getValue)
                        .forEach(x->tupleFreq.put(x,tupleFreq.getOrDefault(x,0)+1));

        List<Map.Entry<Set<ColumnDescriptor>, Integer>> l =
        tupleFreq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());

        l.forEach(x -> System.out.println(x));
        System.out.println("[Tuple Stats]==============================");
    }
    public void printFreqStats() {
        System.out.println("[Column Stats]==============================");
        List<Map.Entry<ColumnDescriptor, Integer>> l =
                freq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
        l.forEach(x -> System.out.println(x.getKey().getTableName()+"."+x.getKey().getColumnName()+": "+x.getValue()));
        System.out.println("[Column Stats]==============================");
    }

    public void printTableFreq() {
        System.out.println("[Table Stats]==============================");
        List<Map.Entry<ColumnDescriptor, Integer>> l =
                tableFreq.entrySet().stream()
                        .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
        l.forEach(x -> System.out.println(x.getKey().getTableName()+": "+x.getValue()));
        System.out.println("[Table Stats]==============================");
    }

    private Map<ColumnDescriptor, Integer> freq = new HashMap<>();
    private Map<SQLContext, Set<ColumnDescriptor>> selectTuples = new HashMap<>();
    private Map<ColumnDescriptor, Integer> tableFreq = new HashMap<>();
}
