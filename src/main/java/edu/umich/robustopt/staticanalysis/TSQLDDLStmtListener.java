package edu.umich.robustopt.staticanalysis;

/**
 * Created by zhxchen on 3/16/16.
 */

import edu.umich.robustopt.staticanalysis.Antlr4TSQLAnalyzerParser.*;

import java.util.*;

public class TSQLDDLStmtListener extends Antlr4TSQLAnalyzerBaseListener {
    public class ColumnInfo {
        private ColumnDescriptor columnId;
        private boolean isForeignKey = false;
        public void setForeignKey() { isForeignKey = true; }

        @Override
        public int hashCode() { return columnId.hashCode(); }

        @Override
        public boolean equals(Object other) { return columnId.equals(other); }

        public ColumnDescriptor getColumnId() { return columnId; }
    }
    public class TableInfo {
        private Map<ColumnDescriptor, ColumnInfo> columns = new LinkedHashMap<>();
        private String tableName;
        public void addColumnInfo(ColumnInfo col) { columns.put(col.getColumnId(), col); }
        public void setForeignKey(ColumnDescriptor col) {

        }
    }

    @Override
    public void enterCreate_table(Create_tableContext ctx) {
        schemaList.add(new TableInfo());
    }
    // TODO reading schemap from ddl, working in progress
    private List<TableInfo> schemaList = new ArrayList<>();
}
