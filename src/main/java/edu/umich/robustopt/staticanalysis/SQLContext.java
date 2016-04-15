package edu.umich.robustopt.staticanalysis;

import sun.util.resources.cldr.de.CalendarData_de_LU;

import java.util.Objects;

/**
 * Created by zhxchen on 4/14/16.
 */
public class SQLContext {
    enum ClauseType {
        NIL, SELECT, WHERE, FROM, GROUPBY, ORDERBY, EXPRESSION, STATEMENT
    }
    enum MinorType {
        NIL, EXTREME, TOTAL, USER, OPERATOR
    }
    SQLContext(ClauseType t) {
        type = t;
        minorType = MinorType.NIL;
        id = nextId++;
    }

    SQLContext(ClauseType t, MinorType m) {
        type = t;
        minorType = m;
        id = nextId++;
    }

    @Override
    public String toString() {
        return "(" + id.toString() + ", " + type + "," + minorType + ")";
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
    MinorType getMinorType() { return minorType; }

    private ClauseType type = ClauseType.NIL;
    private MinorType minorType = MinorType.NIL;
    private Integer id = null;
    static Integer nextId = 0;
}
