package edu.umich.robustopt.staticanalysis;

/**
 * Created by zhxchen on 3/16/16.
 */

import edu.umich.robustopt.staticanalysis.Antlr4TSQLAnalyzerParser.*;
import edu.umich.robustopt.util.SchemaUtils;

import java.util.*;
import java.util.stream.Collectors;

public class TSQLDDLStmtListener extends Antlr4TSQLAnalyzerBaseListener {
    public class ColumnInfo {
        private ColumnDescriptor columnId;
        private boolean isForeignKey = false;
        public void setForeignKey() { isForeignKey = true; }
        public boolean isForeignKey() { return isForeignKey; }
        @Override
        public int hashCode() { return columnId.hashCode(); }
        @Override
        public boolean equals(Object other) { return columnId.equals(other); }
        public ColumnDescriptor getColumnId() { return columnId; }
        public ColumnInfo(ColumnDescriptor col) { columnId = col; }
    }
    public class TableInfo {
        private Map<ColumnDescriptor, ColumnInfo> columns = new LinkedHashMap<>();
        private String tableName;
        public void addColumnInfo(ColumnInfo col) { columns.put(col.getColumnId(), col); }
        public void setForeignKey(ColumnDescriptor col) { columns.get(col).setForeignKey(); }
        public final String getTableName() { return tableName; }
        public final Set<String> getColumnNameSet() {
            return columns.keySet().stream().map(ColumnDescriptor::getColumnName)
                    .collect(Collectors.toSet());
        }
        public TableInfo(String name) { tableName = name; }
    }
    @Override
    public void enterCreate_table(Create_tableContext ctx) {
        schemaList.addLast(new TableInfo(ctx.table_name().getText()));
    }
    @Override
    public void enterColumn_definition(Column_definitionContext ctx) {
        String tableName = schemaList.getLast().getTableName();
        ColumnDescriptor newColumnId = new ColumnDescriptor(SchemaUtils.defaultSchemaName, tableName, ctx.column_name().getText());
        ColumnInfo columnItem = new ColumnInfo(newColumnId);
        schemaList.getLast().addColumnInfo(columnItem);
        lastColumnName = ctx.column_name().getText();
    }
    @Override
    public void exitColumn_definition(Column_definitionContext ctx) {
        lastColumnName = null;
    }
    @Override
    public void enterReference_constraint(Reference_constraintContext ctx) {
        String tableName = schemaList.getLast().getTableName();
        assert(lastColumnName!=null);
        ColumnDescriptor columnId = new ColumnDescriptor(SchemaUtils.defaultSchemaName, tableName, lastColumnName);
        schemaList.getLast().setForeignKey(columnId);
    }
    @Override
    public void enterTable_constraint(Table_constraintContext ctx) {
        // TODO: column_name_list
        if (ctx.column_name()!=null) lastColumnName = ctx.column_name().getText();
    }
    @Override
    public void exitTable_constraint(Table_constraintContext ctx) {
        lastColumnName = null;
    }
    public List<TableInfo> getSchemaList() {
        return schemaList.stream().collect(Collectors.toList());
    }
    public Map<String, Set<String>> getPlainSchemaMap() {
        Map<String, Set<String>> res = new HashMap<>();
        for (TableInfo item : schemaList)
            res.put(item.getTableName(), item.getColumnNameSet());
        return res;
    }

    private String lastColumnName;
    private Deque<TableInfo> schemaList = new ArrayDeque<>();

}
