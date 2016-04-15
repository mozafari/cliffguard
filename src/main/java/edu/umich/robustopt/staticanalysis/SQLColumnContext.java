package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLColumnContext {

    private Pair<String, String> trueName = new Pair<>(null, null);
    private Pair<String, String> queryName;
    private Pair<Integer, Integer> pos = new Pair<>(null, null);
    private Deque<SQLContext> contextStack = new ArrayDeque<>();

    public ColumnDescriptor getColumn() {
        return new ColumnDescriptor(null, trueName.getKey(), trueName.getValue());
    }
    public SQLContext getCurrentContext() {
        return contextStack.peek();
    }

    // TODO move Info class to another
    private void setColumnNameInfo(TSQLSelectStmtListener.ColumnNameInfo c) {
        queryName = new Pair<>(c.getQueryTableName(), c.getQueryColumnName());
        trueName = new Pair<>(c.getTrueTableName(), c.getTrueColumnName());
        pos = c.getPosition();
    }

    private void setContextStack(Deque<SQLContext> ctx) {
        contextStack = ctx;
    }

    @Override
    public String toString() {
        return (queryName.getKey() != null ? queryName.getKey() + "." : "") +
                (queryName.getValue() != null ? queryName.getValue() : "*") +
                " -> " + trueName.getKey() + "." + trueName.getValue() +
                "(" + pos.getKey() + ":" + pos.getValue() + ")";
    }

    static class SQLColumnContextBuilder {
        void setColumnNameInfo(TSQLSelectStmtListener.ColumnNameInfo c) {
            if (ctx==null) ctx = new SQLColumnContext();
            ctx.setColumnNameInfo(c);
        }

        void setContextStack(ArrayDeque<SQLContext> ctxStack) {
            if (ctx==null) ctx = new SQLColumnContext();
            ctx.setContextStack(ctxStack.clone());
        }
        SQLColumnContext build() {
            assert ctx != null;
            SQLColumnContext res = ctx;
            ctx = null;
            return res;
        }
        private SQLColumnContext ctx = null;
    }
}

