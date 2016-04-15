package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLColumnContext {

    private Pair<String, String> trueName = new Pair<>(null, null);
    private Pair<Integer, Integer> pos = new Pair<>(null, null);
    private Deque<SQLContext> contextStack = new ArrayDeque<>();

    public ColumnDescriptor getColumn() {
        return new ColumnDescriptor(null, trueName.getKey(), trueName.getValue());
    }
    public SQLContext getCurrentContext() {
        return contextStack.peek();
    }

    public int size() {
        return contextStack.size();
    }

    public SQLContext getContextByLevel(int level) {
        Iterator<SQLContext> iter = contextStack.descendingIterator();
        SQLContext ret = iter.next();
        while (level>0) {
            ret = iter.next();
            level--;
        }
        return ret;
    }

    public void printContextStack() {
        System.out.println(contextStack);
    }

    // TODO move Info class to another
    private void setColumnName(ColumnDescriptor c) {
        trueName = new Pair<>(c.getTableName(), c.getColumnName());
    }

    private void setPosition(Pair<Integer, Integer> p) {
        pos = p;
    }

    private void setContextStack(Deque<SQLContext> ctx) {
        contextStack = ctx;
    }

    @Override
    public String toString() {
        return trueName.getKey() + "." + trueName.getValue() +
                "(" + pos.getKey() + ":" + pos.getValue() + ")";
    }

    static class SQLColumnContextBuilder {
        void setColumnName(ColumnDescriptor c) {
            if (ctx==null) ctx = new SQLColumnContext();
            ctx.setColumnName(c);
        }

        void setPosition(Pair<Integer, Integer> p) {
            if (ctx==null) ctx = new SQLColumnContext();
            ctx.setPosition(p);
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

