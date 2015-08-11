/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private DownloadSession sessionHandle = null;
  private RecordReader recordReader = null;
  private Object[] rowValues;

  private final int totalRows;
  private int startRow;
  private int fetchedRows;

  /**
   * Whether the query result set is empty.
   * For some meta query (like procedures) we need to return an empty query result.
   */
  private boolean isEmptyResultSet = false;

  /**
   * The maximum number of rows can be fetched from the server.
   * If it equals 0, do not limit.
   */
  private int maxRows;

  /**
   * The number of rows to be fetched from the server each time.
   */
  private int fetchSize;

  /**
   * Tells whether this ResultSet object is scrollable.
   */
  private boolean isScrollable;

  /**
   * Tells the fetch direction for a scrollable ResultSet object.
   */
  private boolean isFetchForward;

  /**
   * A builder class which makes the parameter list of constructor less verbose.
   */
  public static class Builder {

    private OdpsStatement stmtHandle;
    private DownloadSession sessionHandle;
    private OdpsResultSetMetaData meta;
    private int fetchSize = 10000;
    private boolean isFetchForwad = true;
    private boolean isScrollable = false;
    private int maxRows = 0;
    private boolean isEmptyResultSet = false;

    public Builder setStmtHandle(OdpsStatement stmtHandle) {
      this.stmtHandle = stmtHandle;
      return this;
    }

    public Builder setSessionHandle(DownloadSession sessionHandle) {
      this.sessionHandle = sessionHandle;
      return this;
    }

    public Builder setMeta(OdpsResultSetMetaData meta) {
      this.meta = meta;
      return this;
    }

    public Builder setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder setScollable(boolean scollable) {
      this.isScrollable = scollable;
      return this;
    }

    public Builder setFetchForward(boolean fetchForward) {
      this.isFetchForwad = fetchForward;
      return this;
    }

    public Builder setMaxRows(int rows) {
      this.maxRows = rows;
      return this;
    }

    public Builder setEmptyResultSet(boolean isEmpty) {
      this.isEmptyResultSet = isEmpty;
      return this;
    }

    public OdpsQueryResultSet build() throws SQLException {
      return new OdpsQueryResultSet(this);
    }
  }

  OdpsQueryResultSet(Builder builder) throws SQLException {
    super(builder.stmtHandle, builder.meta);
    sessionHandle = builder.sessionHandle;
    fetchSize = builder.fetchSize;
    isFetchForward = builder.isFetchForwad;
    isScrollable = builder.isScrollable;
    maxRows = builder.maxRows;
    isEmptyResultSet = builder.isEmptyResultSet;

    // Initialize the auxiliary variables
    if (!isEmptyResultSet) {
      totalRows = (int) sessionHandle.getRecordCount();
      startRow = 0;
      fetchedRows = 0;
    } else {
      totalRows = 0;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();

    if (!isScrollable) {
      throw new SQLException("Method not supported for TYPE_FORWARD_ONLY resultset");
    }

    fetchedRows = 0;
    startRow = 0;
    recordReader = null;
    rowValues = null;
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();
    return fetchedRows;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    return (fetchedRows == 0);
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
    recordReader = null;
    sessionHandle = null;
    rowValues = null;
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (isEmptyResultSet) {
      return false;
    }

    if (maxRows > 0 && fetchedRows >= maxRows) {
      return false;
    }

    try {
      if (recordReader == null) {
        if (!downloadMoreRows(fetchSize)) {
          return false;
        }
      }

      Record record = recordReader.read();
      if (record == null) {
        if (!downloadMoreRows(fetchSize)) {
          return false;
        } else {
          record = recordReader.read();
          rowValues = record.toArray();
          fetchedRows++;
          return true;
        }
      }

      rowValues = record.toArray();
      fetchedRows++;
      return true;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  /**
   * Download a number of `nextFewRows` records from the session handle.
   *
   * If the `startRows` plus `nextFewRows` exceeds the limit of the rows,
   * a fewer number of rows will be downloaded from the session.
   * Otherwise, a number of `nextFewRows` rows will be downloaded.
   *
   * @param nextFewRows
   *     the number of rows attempts to download
   * @return a boolean that indicates whether the download is successful
   * @throws SQLException
   */
  private boolean downloadMoreRows(int nextFewRows) throws SQLException {
    if (startRow >= totalRows) {
      return false;
    }

    if (startRow + nextFewRows > totalRows) {
      nextFewRows = totalRows - startRow;
    }

    try {
      // TODO: 24 hours limitation
      assert(sessionHandle.getStatus() == TableTunnel.DownloadStatus.NORMAL);
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
  public int getType() throws SQLException {
    checkClosed();

    if (isScrollable) {
      return ResultSet.TYPE_SCROLL_INSENSITIVE;
    } else {
      return ResultSet.TYPE_FORWARD_ONLY;
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkClosed();
    if (rowValues == null) {
      wasNull = true;
      return null;
    }

    Object obj = rowValues[toZeroIndex(columnIndex)];
    wasNull = (obj == null);
    return obj;
  }

  protected int toZeroIndex(int column) {
    if (column <= 0 || column > rowValues.length) {
      throw new IllegalArgumentException();
    }
    return column - 1;
  }
}
