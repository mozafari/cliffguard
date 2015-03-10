package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;

import com.relationalcloud.tsqlparser.schema.Column;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.table.ColumnDefinition;
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


/**
 * Extract the {@link Column} for the current statement
 * 
 * @author fangar
 * 
 */
public class CreateTableExtractColumnsVisitor implements StatementVisitor,
    ColumnReferenceVisitor {

  private ArrayList<Column> columnsFromCreateTable;

  public ArrayList<Column> getColumnsFromCreateTable(CreateTable create) {
    columnsFromCreateTable = new ArrayList<Column>();
    create.accept(this);
    return columnsFromCreateTable;
  }

  @Override
  public void visit(Column column) {
    columnsFromCreateTable.add(column);
  }

  @Override
  public void visit(Insert insert) {
    // TODO Auto-generated method stub
  }

  @Override
  public void visit(Delete arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(CreateTable create) {
    for (ColumnDefinition colDef : create.getColumnDefinitions()) {
      colDef.getColumnName().accept(this);
    }

  }

  @Override
  public void visit(AlterTableStatement arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Replace arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Select arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Update arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Truncate arg0) {
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
