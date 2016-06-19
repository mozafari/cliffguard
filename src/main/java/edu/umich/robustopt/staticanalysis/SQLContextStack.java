package edu.umich.robustopt.staticanalysis;

import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.SchemaUtils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLContextStack {

    private Pair<String, String> trueName = new Pair<>(null, null);
    private Pair<Integer, Integer> pos = new Pair<>(null, null);
    private Deque<SQLContext> contextStack = new ArrayDeque<>();

    public ColumnDescriptor getColumn() {
        return new ColumnDescriptor(SchemaUtils.defaultSchemaName, trueName.getKey(), trueName.getValue());
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
            if (ctx==null) ctx = new SQLContextStack();
            ctx.setColumnName(c);
        }

        void setPosition(Pair<Integer, Integer> p) {
            if (ctx==null) ctx = new SQLContextStack();
            ctx.setPosition(p);
        }

        void setContextStack(ArrayDeque<SQLContext> ctxStack) {
            if (ctx==null) ctx = new SQLContextStack();
            ctx.setContextStack(ctxStack.clone());
        }
        SQLContextStack build() {
            assert ctx != null;
            SQLContextStack res = ctx;
            ctx = null;
            return res;
        }
        private SQLContextStack ctx = null;
    }
}

