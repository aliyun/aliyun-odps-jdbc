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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

public class OdpsForwardResultSet extends OdpsResultSet implements ResultSet {

  private Logger log;
  private DownloadSession sessionHandle;
  private TunnelRecordReader reader = null;
  private Record reuseRecord = null;
  private Object[] currentRow;

  private long fetchedRows = 0;
  private final long totalRows;
  private boolean isClosed = false;

  private long startTime;

  /**
   * For logging the time consumption for fetching 10000 rows
   */
  private static final long ACCUM_FETCHED_ROWS = 10000;
  long accumTime;
  long accumKBytes = 0;

  /**
   * The maximum retry time we allow to tolerate the network problem
   */
  private static final int READER_REOPEN_TIME_MAX = 5;

  OdpsForwardResultSet(OdpsStatement stmt, OdpsResultSetMetaData meta, DownloadSession session)
      throws SQLException {
    super(stmt, meta);
    log = stmt.getParentLogger();
    sessionHandle = session;
    int maxRows = stmt.resultSetMaxRows;
    long recordCount = sessionHandle.getRecordCount();

    // maxRows take effect only if it > 0
    if (maxRows > 0 && maxRows <= recordCount) {
      totalRows = maxRows;
    } else {
      totalRows = recordCount;
    }

    startTime = System.currentTimeMillis();
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
    sessionHandle = null;
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
    log.fine("the result set has been closed");
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (fetchedRows == totalRows) {
      long end = System.currentTimeMillis();
      log.fine("It took me " + (end - startTime) + " ms to fetch all records");
      // For forward result set, we implicitly close it after fetching all records
      close();
      return false;
    }

    // Tolerate the network problem by simply reopen the reader
    int retry = 0;
    while (true) {
      try {
        if (reader == null) {
          rebuildReader();
          accumTime = System.currentTimeMillis();
        }
        reuseRecord = reader.read(reuseRecord);
        int columns = reuseRecord.getColumnCount();
        currentRow = new Object[columns];
        for (int i = 0; i < columns; i++) {
          currentRow[i] = reuseRecord.get(i);
        }

        fetchedRows++;

        // Log the time consumption for fetching a bunch of rows
        if (fetchedRows % ACCUM_FETCHED_ROWS == 0 && fetchedRows != 0) {
          long delta = reader.getTotalBytes() / 1024 - accumKBytes;
          long duration = System.currentTimeMillis() - accumTime;
          log.fine(String.format("fetched %d rows, %d KB, %.2f KB/s", ACCUM_FETCHED_ROWS,
                                  delta, (float) delta / duration * 1000));

          accumKBytes = reader.getTotalBytes() / 1024;
          accumTime = System.currentTimeMillis();
        }
        return true;
      } catch (IOException e) {
        log.info("read from a bad file, retry=" + retry);
        if (++retry == READER_REOPEN_TIME_MAX) {
          throw new SQLException("to much retries because: " + e.getMessage());
        }
        rebuildReader();
      }
    }
  }

  /**
   * Rebuild a reader to read the remainder of records from the cursor
   *
   * @throws SQLException
   */
  private void rebuildReader() throws SQLException {
    try {
      long count = totalRows - fetchedRows;
      reader = sessionHandle.openRecordReader(fetchedRows, count, true);
      log.fine(String.format("open read record, start=%d, cnt=%d", fetchedRows, count));
    } catch (IOException e) {
      throw new SQLException(e);
    } catch (TunnelException e) {
      throw new SQLException(e);
    }
  }

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
