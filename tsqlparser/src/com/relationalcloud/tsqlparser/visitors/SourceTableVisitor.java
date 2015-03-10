package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;
import java.util.List;

import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsListVisitor;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.drop.DropTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.replace.Replace;
import com.relationalcloud.tsqlparser.statement.select.FromItemVisitor;
import com.relationalcloud.tsqlparser.statement.select.Join;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.select.SelectVisitor;
import com.relationalcloud.tsqlparser.statement.select.SubJoin;
import com.relationalcloud.tsqlparser.statement.select.SubSelect;
import com.relationalcloud.tsqlparser.statement.select.Union;
import com.relationalcloud.tsqlparser.statement.truncate.Truncate;
import com.relationalcloud.tsqlparser.statement.update.Update;


public class SourceTableVisitor implements StatementVisitor, FromItemVisitor,
    SelectVisitor, ItemsListVisitor {

  private ArrayList<Table> sourceTables;

  public ArrayList<Table> getTablesFromInsert(Insert insert) {
    sourceTables = new ArrayList<Table>();
    insert.accept(this);
    return sourceTables;
  }

  public void visit(SubSelect subSelect) {
    subSelect.getSelectBody().accept(this);
  }

  @SuppressWarnings("unchecked")
  public void visit(PlainSelect plainSelect) {
    plainSelect.getFromItem().accept(this);

    if (plainSelect.getJoins() != null) {
      List<Join> joins = plainSelect.getJoins();
      for (Join j : joins) {
        j.getRightItem().accept(this);
      }
    }
  }

  public void visit(Table table) {
    sourceTables.add(table);
  }

  @Override
  public void visit(Insert insert) {
    insert.getItemsList().accept(this);
  }

  @Override
  public void visit(SubJoin subjoin) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Union union) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(ExpressionList expressionList) {
    // TODO Auto-generated method stub

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

}
