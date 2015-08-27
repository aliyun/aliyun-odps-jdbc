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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private DownloadSession sessionHandle = null;
  private int fetchSize;
  private boolean isScrollable;
  private boolean isFetchForward;

  /**
   * Keeps in the memory a frame of rows which are likely be accessed in the near future.
   */
  private Object[][] rowsCache;

  private final long totalRows;

  /**
   * The range of cursorRow is from -1 to totalRows
   *
   * [0, totalRows] indicated an effective row
   * cursorRow == -1 indicates a beforeFirst row
   * cursorRow == totalRows indicated an afterLast row
   */
  private long cursorRow;

  /**
   * Marks the upper bound of the record frame which has been cached.
   *
   * cachedUpperRow == totalRows simply means there are no cached records.
   */
  private long cachedUpperRow;

  private boolean isClosed = false;

  private static Log log = LogFactory.getLog(OdpsQueryResultSet.class);

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session)
      throws SQLException {
    super(stmt, meta);
    sessionHandle = session;
    fetchSize = stmt.resultSetFetchSize;
    isFetchForward = stmt.isResultSetFetchForward;
    isScrollable = stmt.isResultSetScrollable;
    int maxRows = stmt.resultSetMaxRows;

    long recordCount = sessionHandle.getRecordCount();

    // maxRows take effect only if it > 0
    if (maxRows > 0 && maxRows <= recordCount) {
      totalRows = maxRows;
    } else {
      totalRows = recordCount;
    }
    cachedUpperRow = totalRows;

    // For a FETCH_FORWARD resultset, the cursor will be -1 initially.
    // For a FETEH_REVERSE resultset, the cursor will be initialized to `totalRows`.
    cursorRow = isFetchForward ? -1 : totalRows;
    rowsCache = new Object[fetchSize][];
  }

  @Override
  public boolean absolute(int rows) throws SQLException {
    checkClosed();

    long target = rows >= 0 ? rows : totalRows + rows;
    target = rows >= 0 ? target - 1 : target;  // to zero-index

    if (target >= 0 && target < totalRows) {
      cursorRow = target;
      return true;
    }

    // leaves the cursor beforeFirst or afterLast
    cursorRow = target < 0 ? -1 : totalRows;
    return false;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClosed();
    cursorRow = totalRows;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("The result set has been closed");
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClosed();
    cursorRow = -1;
  }

  @Override
  public boolean first() throws SQLException {
    checkClosed();
    cursorRow = 0;
    return totalRows > 0;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return isFetchForward ? FETCH_FORWARD : FETCH_REVERSE;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
    rowsCache = new Object[fetchSize][];  // realloc memory
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();
    return (int) cursorRow + 1;
  }

  @Override
  public int getType() throws SQLException {
    if (isScrollable) {
      return ResultSet.TYPE_SCROLL_INSENSITIVE;
    } else {
      return ResultSet.TYPE_FORWARD_ONLY;
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();
    return (cursorRow == totalRows);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    return (cursorRow == -1);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();
    return (cursorRow == 0);
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();
    return (cursorRow == totalRows - 1);
  }

  @Override
  public boolean last() throws SQLException {
    checkClosed();
    cursorRow = totalRows;
    return totalRows > 0;
  }

  @Override
  public boolean previous() throws SQLException {
    checkClosed();

    if (cursorRow != -1) {
      cursorRow--;
    }
    return (cursorRow != -1);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClosed();

    long target = cursorRow + rows;
    if (target >= 0 && target < totalRows) {
      cursorRow = target;
      return true;
    }

    // leaves the cursor beforeFirst or afterLast
    cursorRow = target < 0 ? -1 : totalRows;
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    switch (direction) {
      case FETCH_FORWARD:
        isFetchForward = true;
      case FETCH_REVERSE:
        isFetchForward = false;
      default:
        throw new SQLException("Only FETCH_FORWARD and FETCH_REVERSE is valid");
    }
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
    sessionHandle = null;
    rowsCache = null;
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (cursorRow != totalRows) {
      cursorRow++;
    }
    return cursorRow != totalRows;
  }

  protected Object[] rowAtCursor() throws SQLException {
    // detect whether the cache contains the record
    boolean cacheHit = (cursorRow >= cachedUpperRow) && (cursorRow < cachedUpperRow + fetchSize);
    if (!cacheHit) {
      fetchRows();
    }

    int offset = (int) (cursorRow - cachedUpperRow);
    return rowsCache[offset];
  }

  /**
   * Fetch into buffer from the session a frame of records in where cursorRow appears.
   */
  private void fetchRows() throws SQLException {
    // determines the frame id to be cached
    cachedUpperRow = (cursorRow / fetchSize) * fetchSize;

    // tailor the fetchSize to read effective records
    long count = fetchSize;
    if (cachedUpperRow + count > totalRows) {
      count = totalRows - cachedUpperRow;
    }

    try {
      Record reuseRecord = null;
      TunnelRecordReader reader = sessionHandle.openRecordReader(cachedUpperRow, count);
      for (int i = 0; i < count; i++) {
        reuseRecord = reader.read(reuseRecord);
        int columns = reuseRecord.getColumnCount();
        rowsCache[i] = new Object[columns];
        for (int j = 0; j < reuseRecord.getColumnCount(); j++) {
          rowsCache[i][j] = reuseRecord.get(j);
        }
      }
      log.debug(String.format("read record, start=%d, cnt=%d, %dKB", cachedUpperRow, count,
                              reader.getTotalBytes() / 1024));
      reader.close();
    } catch (TunnelException e) {
      throw new SQLException(e);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
