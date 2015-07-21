package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Iterator;

import com.aliyun.odps.Function;

public class OdpsFunctionsResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Function> iterator;
  private Function function;

  OdpsFunctionsResultSet(Iterator<Function> iterator, OdpsResultSetMetaData meta)
      throws SQLException {
    super(null, meta);
    this.iterator = iterator;
  }

  @Override
  public boolean next() throws SQLException {
    if (iterator.hasNext()) {
      function = iterator.next();
      return true;
    } else {
      function = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    switch (columnIndex) {
      case 1: // FUNCTION_CAT
        return null;
      case 2: // FUNCTION_SCHEM
        return null;
      case 3: // FUNCTION_NAME
        return function.getName();
      case 4: // REMARKS
        return null;
      case 5: // FUNCTION_TYPE
        return 0;   // TODO: set a more reasonable value
      case 6: // SPECIFIC_NAME
        return null;
      default:
        return null;
    }
  }
}
