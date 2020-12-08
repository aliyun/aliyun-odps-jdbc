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

import com.aliyun.odps.data.Record;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OdpsSessionForwardResultSet extends OdpsResultSet implements ResultSet {

  private Object[] currentRow;
  com.aliyun.odps.data.ResultSet resultSet;
  private int fetchedRows = 0;
  // max row count can be read
  private int totalRows = Integer.MAX_VALUE;
  private boolean isClosed = false;

  private long startTime;


  OdpsSessionForwardResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, com.aliyun.odps.data.ResultSet resultSet, long startTime)
      throws SQLException {
    super(stmt.getConnection(), stmt, meta);

    // maxRows take effect only if it > 0
    if (stmt.resultSetMaxRows > 0) {
      totalRows = stmt.resultSetMaxRows;
    }

    this.resultSet = resultSet;
    this.startTime = startTime;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("The result set has been closed");
    }
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();
    return (int) fetchedRows;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    isClosed = true;
    conn.log.debug("the result set has been closed");
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (fetchedRows == totalRows || !resultSet.hasNext()) {
      conn.log.info("It took me " + (System.currentTimeMillis() - startTime)
                    + " ms to fetch all records, count:" + fetchedRows);
      return false;
    }
    Record record = resultSet.next();
    int columns = record.getColumnCount();
    currentRow = new Object[columns];
    for (int i = 0; i < columns; i++) {
      currentRow[i] = record.get(i);
    }
    fetchedRows++;
    return true;
  }

  @Override
  protected Object[] rowAtCursor() throws SQLException {
    if (currentRow == null) {
      throw new SQLException("the row should be not-null, row=" + fetchedRows);
    }

    if (currentRow.length == 0) {
      throw new SQLException("the row should have more than 1 column , row=" + fetchedRows);
    }

    return currentRow;
  }
}