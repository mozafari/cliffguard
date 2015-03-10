package com.relationalcloud.tsqlparser.visitors;

import java.util.ArrayList;
import java.util.List;

import com.relationalcloud.tsqlparser.expression.operators.relational.ExpressionList;
import com.relationalcloud.tsqlparser.expression.operators.relational.ItemsListVisitor;
import com.relationalcloud.tsqlparser.schema.Table;
import com.relationalcloud.tsqlparser.statement.Statement;
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


/**
 * Return the tables for the current statement.
 */
public class TablesForStatementVisitor implements StatementVisitor,
    ItemsListVisitor, SelectVisitor, FromItemVisitor {

  private ArrayList<Table> tablesForStatement;

  public ArrayList<Table> getAffectedTablesFromStatement(Statement stmt) {
    tablesForStatement = new ArrayList<Table>();
    stmt.accept(this);
    return tablesForStatement;
  }

  @Override
  public void visit(Insert insert) {
    tablesForStatement.add(insert.getTable());
    insert.getItemsList().accept(this);
  }

  public void visit(SubSelect subSelect) {
    subSelect.getSelectBody().accept(this);
  }

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
    tablesForStatement.add(table);
  }

  public void visit(DropTable drop) {
    for (Table table : drop.getTable()) {
      tablesForStatement.add(table);
    }
  }

  @Override
  public void visit(Delete delete) {
    for (Table table : delete.getTables()) {
      tablesForStatement.add(table);
    }
  }

  @Override
  public void visit(CreateTable create) {
    tablesForStatement.add(create.getTable());
  }

  @Override
  public void visit(AlterTableStatement alterTable) {
    tablesForStatement.add(alterTable.getTable());
  }

  @Override
  public void visit(Replace arg0) {
    tablesForStatement.add(arg0.getTable());
  }

  @Override
  public void visit(Update arg0) {
    for (Table t : (List<Table>) arg0.getTables())
      tablesForStatement.add(t);

  }

  @Override
  public void visit(Truncate arg0) {
    tablesForStatement.add(arg0.getTable());

  }

  @Override
  public void visit(SubJoin arg0) {
    arg0.accept(this);
  }

  @Override
  public void visit(Union arg0) {
    List<PlainSelect> l = arg0.getPlainSelects();
    for (PlainSelect s : l) {
      s.accept(this);

    }

  }

  @Override
  public void visit(ExpressionList arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Select select) {
    // TODO Auto-generated method stub
    select.getSelectBody().accept(this);
  }

}
