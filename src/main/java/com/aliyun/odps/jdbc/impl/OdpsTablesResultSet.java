package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;

import com.aliyun.odps.Table;

public class OdpsTablesResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Table> iterator;

  private OdpsResultSetMetaData meta;

  private Table table;

  OdpsTablesResultSet(Iterator<Table> iterator) throws SQLException {
    super(null);
    this.iterator = iterator;
  }

  @Override
  public boolean next() throws SQLException {
    boolean hasNext = iterator.hasNext();

    if (hasNext) {
      table = iterator.next();
    } else {
      table = null;
    }

    return hasNext;
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public OdpsResultSetMetaData getMetaData() throws SQLException {
    if (meta != null) {
      return meta;
    }

    String[] columns =
        new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
            "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
    int[] types = new int[columns.length];
    Arrays.fill(types, Types.VARCHAR);

    meta = new OdpsResultSetMetaData(columns, types);

    return meta;
  }

  @Override
  Object getObject0(int columnIndex) throws SQLException {
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
