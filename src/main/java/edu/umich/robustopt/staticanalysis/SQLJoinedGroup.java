package edu.umich.robustopt.staticanalysis;

/**
 * Created by sorrow17 on 2016/5/26.
 */
public class SQLJoinedGroup {
    enum JoinType {
        INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, SELF
    }
    enum Relationship {
        MANY_TO_MANY, FOREIGN_KEY, ONE_TO_ONE
    }

    @Override
    public int hashCode() {
        return columns.hashCode() + joinType.hashCode() + relationship.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SQLJoinedGroup)) return false;
        SQLJoinedGroup otherGroup = (SQLJoinedGroup) other;
        return columns.equals(otherGroup.columns) && joinType.equals(otherGroup.joinType) && relationship.equals(otherGroup.relationship);
    }
    @Override
    public String toString() {
        return "["+columns.toString()+", "+joinType+", "+relationship+"]";
    }
    void addColumn(ColumnDescriptor col) {
        columns.add(col);
    }
    void setRelationship(Relationship rel) {
        relationship = rel;
    }
    SQLColumnSet getColumnSet() { return columns; }
    SQLJoinedGroup(SQLColumnSet cols, JoinType jt, Relationship rel) {
        columns = cols;
        joinType = jt;
        relationship = rel;
    }

    private SQLColumnSet columns;
    private JoinType joinType;
    private Relationship relationship;
}
