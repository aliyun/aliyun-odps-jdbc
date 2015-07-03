package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class OdpsResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {

  final String[] columns;
  final int[] types;

  private Map<String, Integer> columnMap;

  OdpsResultSetMetaData(String[] columns, int[] types) {
    this.columns = columns;
    this.types = types;
  }

  int getColumnIndex(String name) {
    if (columnMap == null) {
      columnMap = new HashMap<String, Integer>();
      for (int i = 0; i < columns.length; ++i) {
        columnMap.put(columns[i], i + 1);
        columnMap.put(columns[i].toLowerCase(), i + 1);
      }
    }

    Integer index = columnMap.get(name);
    if (index == null) {
      String lowerName = name.toLowerCase();
      if (lowerName == name) {
        return -1;
      }

      index = columnMap.get(name);
    }

    if (index == null) {
      return -1;
    }

    return index.intValue();
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.length;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {

    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {

    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {

    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {

    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {

    return 0;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {

    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {

    return 0;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {

    return null;
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    checkIndex(column);
    return columns[column - 1];
  }

  @Override
  public String getSchemaName(int column) throws SQLException {

    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {

    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {

    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {

    return null;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {

    return null;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    checkIndex(column);
    return types[column - 1];
  }

  protected void checkIndex(int column) {
    if (column <= 0 || column > columns.length) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {

    return null;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {

    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {

    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {

    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {

    return null;
  }

}
