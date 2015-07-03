package com.aliyun.odps.jdbc.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.csvreader.CsvReader;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private CsvReader resultReader;
  private OdpsResultSetMetaData meta;

  private String[] values;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, CsvReader resultReader)
      throws SQLException {
    super(stmt);
    this.meta = meta;
    this.resultReader = resultReader;
  }

  @Override
  public boolean next() throws SQLException {
    try {
      boolean result = resultReader.readRecord();

      if (result) {
        values = resultReader.getValues();
      } else {
        values = null;
      }

      return result;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public OdpsResultSetMetaData getMetaData() throws SQLException {
    return meta;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return values[columnIndex - 1];
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    int columnIndex = meta.getColumnIndex(columnLabel);
    return getObject(columnIndex);
  }

  protected void checkIndex(int column) {
    if (column <= 0 || column > values.length) {
      throw new IllegalArgumentException();
    }
  }
}
