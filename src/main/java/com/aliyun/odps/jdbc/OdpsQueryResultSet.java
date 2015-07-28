package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private static final long DOWNLOAD_ROWS_STEP = 10000;

  private RecordReader recordReader = null;
  private DownloadSession sessionHandle;
  private Object[] rowValues;

  private long startRow;
  private long totalRows;
  private Integer fetchDirection;
  private Integer fetchSize;

  private Boolean isClosed = false;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session)
      throws SQLException {
    super(stmt, meta);
    this.sessionHandle = session;
    totalRows = sessionHandle.getRecordCount();
    startRow = 0;
  }

  /**
   * Download a number of `nextFewRows` records from the session handle.
   *
   * If the `startRows` plus `nextFewRows` exceeds the limit of the rows,
   * a fewer number of rows will be downloaded from the session.
   * Otherwise, a number of `nextFewRows` rows will be downloaded.
   *
   * @param nextFewRows the number of rows attempts to download
   * @return
   * @throws SQLException
   */
  private boolean downloadMoreRows(long nextFewRows) throws SQLException {
    if (startRow >= totalRows) {
      return false;
    }

    if (startRow + nextFewRows > totalRows) {
      nextFewRows = totalRows - startRow;
    }

    try {
      recordReader = sessionHandle.openRecordReader(startRow, nextFewRows);
      startRow += nextFewRows;
      return true;
    } catch (TunnelException e) {
      throw new SQLException(
          "can not open record reader: " + startRow + ":" + nextFewRows, e);
    } catch (IOException e) {
      throw new SQLException(
          "can not open record reader: " + startRow + ":" + nextFewRows, e);
    }
  }

  @Override
  public boolean next() throws SQLException {

    if (recordReader == null) {
      if (!downloadMoreRows(DOWNLOAD_ROWS_STEP)) {
        return false;
      }
    }

    try {
      Record record = recordReader.read();
      if (record == null) {
        if (!downloadMoreRows(DOWNLOAD_ROWS_STEP)) {
          return false;
        } else {
          record = recordReader.read();
          rowValues = record.toArray();
          return true;
        }
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
