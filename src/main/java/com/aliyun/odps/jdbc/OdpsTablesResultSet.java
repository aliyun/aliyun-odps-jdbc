package com.aliyun.odps.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import com.aliyun.odps.Table;

public class OdpsTablesResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Table> iterator;
  private Table table;

  OdpsTablesResultSet(Iterator<Table> iterator, OdpsResultSetMetaData meta)
      throws SQLException {
    super(null, meta);
    this.iterator = iterator;
  }

  @Override
  public boolean next() throws SQLException {
    if (iterator.hasNext()) {
      table = iterator.next();
      return true;
    } else {
      table = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    switch (columnIndex) {
      case 1:
        return table.getProject();
      case 2:
        return null;
      case 3:
        return table.getName();
      case 4:
        return table.isVirtualView() ? "VIEW" : "TABLE";
      case 5:
        return table.getComment();
      case 6: // TYPE_CAT
        return null;
      case 7: // TYPE_SCHEM
        return null;
      case 8: // TYPE_NAME
        return null;
      case 9: // SELF_REFERENCING_COL_NAME
        return null;
      case 10: // REF_GENERATION
        return "USER";
      default:
        return null;
    }
  }

}
