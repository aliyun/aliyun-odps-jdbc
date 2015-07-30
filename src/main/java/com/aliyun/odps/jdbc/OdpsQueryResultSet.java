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
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private RecordReader recordReader = null;
  private DownloadSession sessionHandle;
  private Object[] rowValues;

  private int startRow;
  private final int totalRows;
  private int fetchedRows;

  /**
   * The number of rows to be fetched for each time.
   */
  private int fetchSize;

  /**
   * A boolean is sufficient to tell the type among:
   * TYPE_FORWARD_ONLY:        false
   * TYPE_SCROLL_INSENSITIVE: true
   * TYPE_SCROLL_SENSITIVE:    not supported
   */
  private boolean isScrollable = false;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session,
                     boolean scrollable)
      throws SQLException {
    super(stmt, meta);
    this.sessionHandle = session;
    totalRows = (int) sessionHandle.getRecordCount();
    startRow = 0;
    fetchedRows = 0;
    fetchSize = stmt.getFetchSize();
    isScrollable = scrollable;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();
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
   * @return
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
