package com.relationalcloud.tsqlparser.visitors;

import java.util.List;

import com.relationalcloud.tsqlparser.statement.Statement;
import com.relationalcloud.tsqlparser.statement.StatementVisitor;
import com.relationalcloud.tsqlparser.statement.alter.table.AlterTableStatement;
import com.relationalcloud.tsqlparser.statement.create.table.CreateTable;
import com.relationalcloud.tsqlparser.statement.delete.Delete;
import com.relationalcloud.tsqlparser.statement.drop.DropTable;
import com.relationalcloud.tsqlparser.statement.insert.Insert;
import com.relationalcloud.tsqlparser.statement.replace.Replace;
import com.relationalcloud.tsqlparser.statement.select.PlainSelect;
import com.relationalcloud.tsqlparser.statement.select.Select;
import com.relationalcloud.tsqlparser.statement.select.SelectVisitor;
import com.relationalcloud.tsqlparser.statement.select.Union;
import com.relationalcloud.tsqlparser.statement.truncate.Truncate;
import com.relationalcloud.tsqlparser.statement.update.Update;


public class OrderByVisitor implements StatementVisitor, SelectVisitor {

  private List orderBy = null;

  public List getOrderBy(Statement s) {
    s.accept(this);
    return orderBy;
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    orderBy = plainSelect.getOrderByElements();
  }

  @Override
  public void visit(Union union) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(Insert insert) {
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
    select.getSelectBody().accept(this);
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
