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

  private static final long DOWNLOAD_ROWS_STEP = 10000;

  private RecordReader recordReader = null;
  private DownloadSession sessionHandle;
  private Object[] rowValues;

  private long startRow;
  private long totalRows;
  private Integer fetchDirection;
  private Integer fetchSize;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session)
      throws SQLException {
    super(stmt, meta);
    this.sessionHandle = session;
    totalRows = sessionHandle.getRecordCount();
    startRow = 0;
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
    fetchDirection = null;
    fetchSize = null;
    recordReader = null;
    sessionHandle = null;
    rowValues = null;
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
    checkClosed();

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
    checkClosed();

    if (fetchDirection == null) {
      return stmt.getFetchDirection();
    }

    return fetchDirection;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();

    fetchDirection = direction;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClosed();

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
