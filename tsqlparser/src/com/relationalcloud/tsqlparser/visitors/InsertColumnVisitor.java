package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.drop.DropTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.replace.Replace;
import com.relationalcloud.tsqlparser.statement.select.ColumnIndex;
import com.relationalcloud.tsqlparser.statement.select.ColumnReferenceVisitor;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.truncate.Truncate;
import com.relationalcloud.tsqlparser.statement.update.Update;


public class InsertColumnVisitor implements StatementVisitor,
    ColumnReferenceVisitor {

  private ArrayList<Column> insertColumn;

  public ArrayList<Column> getInsertColumn(Insert insert) {
    this.insertColumn = new ArrayList<Column>();
    insert.accept(this);
    return insertColumn;
  }

  @Override
  public void visit(Insert insert) {
    if (insert.getColumns() != null && insert.getColumns().size() > 0) {
      for (Column column : insert.getColumns()) {
        column.accept(this);
      }
    }
  }

  @Override
  public void visit(Column column) {
    this.insertColumn.add(column);
  }

  @Override
  public void visit(Delete delete) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(CreateTable create) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(AlterTableStatement alter) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Replace replace) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Select select) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Update update) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Truncate truncate) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(DropTable drop) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(ColumnIndex columnIndex) {
    // TODO Auto-generated method stub

  }
}
