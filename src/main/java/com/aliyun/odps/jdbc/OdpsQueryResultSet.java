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
import java.util.logging.Logger;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

public class OdpsQueryResultSet extends OdpsResultSet implements ResultSet {

  private Logger log;
  private DownloadSession sessionHandle;
  private int fetchSize;
  private OdpsStatement.FetchDirection fetchDirection;
  private final long totalRows;

  /**
   * Keeps in the memory a frame of rows which are likely be accessed in the near future.
   */
  private Object[][] rowsCache;

  /**
   * The range of cursorRow is from -1 to totalRows. -1 indicates a beforeFirst row
   * while totalRows indicates an afterLast row.
   */
  private long cursorRow;

  /**
   * Marks the upper bound of the record frame which has been cached.
   *
   * cachedUpperRow == totalRows simply means there are no cached records.
   */
  private long cachedUpperRow;

  private boolean isClosed = false;

  OdpsQueryResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session)
      throws SQLException {
    super(stmt, meta);
    log = stmt.getParentLogger();
    sessionHandle = session;
    fetchSize = stmt.resultSetFetchSize;
    fetchDirection = stmt.resultSetFetchDirection;
    int maxRows = stmt.resultSetMaxRows;

    long recordCount = sessionHandle.getRecordCount();

    // maxRows take effect only if it > 0
    if (maxRows > 0 && maxRows <= recordCount) {
      totalRows = maxRows;
    } else {
      totalRows = recordCount;
    }
    cachedUpperRow = totalRows;
    cursorRow = -1;
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
    int direction;
    switch (fetchDirection) {
      case FORWARD:
        direction = ResultSet.FETCH_FORWARD;
        break;
      case REVERSE:
        direction = ResultSet.FETCH_REVERSE;
        break;
      default:
        direction = ResultSet.FETCH_UNKNOWN;
    }
    return direction;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
    rowsCache = new Object[fetchSize][];  // realloc memory
    cachedUpperRow = totalRows;
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();
    return (int) cursorRow + 1;
  }

  @Override
  public int getType() throws SQLException {
    return TYPE_SCROLL_INSENSITIVE;
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
    cursorRow = totalRows - 1;
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
      case ResultSet.FETCH_FORWARD:
        fetchDirection = OdpsStatement.FetchDirection.FORWARD;
        break;
      case ResultSet.FETCH_REVERSE:
        fetchDirection = OdpsStatement.FetchDirection.REVERSE;
        break;
      case ResultSet.FETCH_UNKNOWN:
        fetchDirection = OdpsStatement.FetchDirection.UNKNOWN;
        break;
      default:
        throw new SQLException("invalid argument for setFetchDirection()");
    }
    log.info("setFetchDirection has not been utilized");
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
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

    Object[] row = rowsCache[offset];

    if (row == null) {
      throw new SQLException("the row should be not-null, row=" + cursorRow);
    }

    if (row.length == 0) {
      throw new SQLException("the row should have more than 1 column , row=" + cursorRow);
    }

    return row;
  }

  /**
   * Fetch into buffer from the session a frame of records where cursorRow locates.
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
      long start = System.currentTimeMillis();
      Record reuseRecord = null;
      TunnelRecordReader reader = sessionHandle.openRecordReader(cachedUpperRow, count, true);
      for (int i = 0; i < count; i++) {
        reuseRecord = reader.read(reuseRecord);
        int columns = reuseRecord.getColumnCount();
        rowsCache[i] = new Object[columns];
        for (int j = 0; j < reuseRecord.getColumnCount(); j++) {
          rowsCache[i][j] = reuseRecord.get(j);
        }
      }
      long duration = System.currentTimeMillis() - start;
      long totalKBytes = reader.getTotalBytes() / 1024;
      log.fine(String.format("fetch records, start=%d, cnt=%d, %d KB, %.2f KB/s", cachedUpperRow,
                              count, totalKBytes, (float) totalKBytes / duration * 1000));
      reader.close();
    } catch (TunnelException e) {
      throw new SQLException(e);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
