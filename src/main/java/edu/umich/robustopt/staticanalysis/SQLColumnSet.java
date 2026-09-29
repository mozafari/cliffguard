package edu.umich.robustopt.staticanalysis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sorrow17 on 2016/5/26.
 */
class SQLColumnSet {
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
    public void addAll(SQLColumnSet other) { cols.addAll(other.cols); }
    public boolean containsAll(SQLColumnSet other) { return cols.containsAll(other.cols); }
    public boolean contains(ColumnDescriptor c) { return cols.contains(c); }
    public void remove(ColumnDescriptor c) { cols.remove(c); }

    public Integer size() { return cols.size(); }
    @Override
    public int hashCode() {
        return cols.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof SQLColumnSet) && ((SQLColumnSet) o).cols.equals(cols);
    }

    public List<ColumnDescriptor> getColumns() { return cols.stream().collect(Collectors.toList()); }

    public SQLColumnSet() {}
    public SQLColumnSet(Set<ColumnDescriptor> c) { cols = c; }

    private Set<ColumnDescriptor> cols = new HashSet<>();
}
