package com.aliyun.odps.jdbc.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private RecordReader recordReader;
  private Object[] rowValues;

  private Integer fetchDirection;
  private Integer fetchSize;

  private Boolean isClosed = false;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, RecordReader reader)
      throws SQLException {
    super(stmt, meta);
    this.recordReader = reader;
  }

  @Override
  public boolean next() throws SQLException {
    try {
      Record record = recordReader.read();
      if (record == null) {
        wasNull = true;
        return false;
      }
      rowValues = record.toArray();
      return true;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    if (fetchDirection == null) {
      return stmt.getFetchDirection();
    }

    return fetchDirection;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    fetchDirection = direction;
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (fetchSize == null) {
      return stmt.getFetchSize();
    }

    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  @Override
  public void close() throws SQLException {
    if (!isClosed() && stmt != null) {
      stmt.close();
    }

    isClosed = true;
    stmt = null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return rowValues != null ? rowValues[toZeroIndex(columnIndex)] : null;
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    int columnIndex = getMetaData().getColumnIndex(columnName);
    return getObject(columnIndex);
  }

  protected int toZeroIndex(int column) {
    if (column <= 0 || column > rowValues.length) {
      throw new IllegalArgumentException();
    }
    return column - 1;
  }
}
