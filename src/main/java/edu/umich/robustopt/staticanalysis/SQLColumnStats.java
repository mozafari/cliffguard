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
        SQLContext queryCtx = ctx.getCurrentContext();
        System.out.println(queryCtx.getType());
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
        List<Set<ColumnDescriptor>> l =
                selectTuples.entrySet().stream()
                .map(Map.Entry::getValue).collect(Collectors.toList());
        l.forEach(System.out::println);
    }
    public void printFreqStats() {
        List<Map.Entry<ColumnDescriptor, Integer>> l =
                freq.entrySet().stream()
                .sorted((x,y)->y.getValue()-x.getValue()).collect(Collectors.toList());
        l.forEach(System.out::println);
    }
    private Map<ColumnDescriptor, Integer> freq = new HashMap<>();
    private Map<SQLContext, Set<ColumnDescriptor>> selectTuples = new HashMap<>();
}
