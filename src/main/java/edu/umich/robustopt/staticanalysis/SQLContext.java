package edu.umich.robustopt.staticanalysis;

import java.util.Objects;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLContext {
    enum ClauseType {
        NIL, SELECT, WHERE, FROM, GROUPBY, ORDERBY, EXPRESSION
    }
    SQLContext(ClauseType t) {
        type = t;
        id = nextId++;
    }

    @Override
    public int hashCode() { return id; }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SQLContext)
            return ((SQLContext)other).id.equals(id);
        else
            return false;
    }

    ClauseType getType() { return type; }
    private ClauseType type = ClauseType.NIL;
    private Integer id = null;
    static Integer nextId = 0;
}
