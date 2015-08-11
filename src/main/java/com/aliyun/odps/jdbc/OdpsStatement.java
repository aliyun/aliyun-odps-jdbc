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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsStatement extends WrapperAdapter implements Statement {

  private OdpsConnection conn;
  private Instance instance = null;
  private ResultSet resultSet = null;
  private int updateCount = -1;
  private boolean isClosed = false;
  private boolean isCancelled = false;

  /**
   * Sets the scrollablity of ResultSet objects produced by this statement.
   */
  protected boolean isResultSetScrollable = false;

  /**
   * Sets the fetch direction of ResultSet objects produced by this statement.
   */
  private boolean isResultSetFetchForward = true;

  /**
   * Sets the limits of row numbers that a ResultSet object produced by this statement
   * can contain. If maxRows equals 0, then there is no limits.
   */
  private int maxRows = 0;

  /**
   * Sets the number of rows to be fetched from the server each time.
   * It is just a hint which can be ignored by the implementation of ResultSet.
   */
  private int fetchSize = 10000;

  /**
   * Used to create a temp table for producing ResultSet object.
   */
  private final String tempTableName;

  private SQLWarning warningChain = null;

  OdpsStatement(OdpsConnection conn) {
    this(conn, false);
  }

  OdpsStatement(OdpsConnection conn, boolean isResultSetScrollable) {
    this.conn = conn;
    this.isResultSetScrollable = isResultSetScrollable;
    tempTableName = "jdbc_temp_tbl_" + UUID.randomUUID().toString().replaceAll("-", "_");
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * TODO: should we support cancel?
   *
   * @throws SQLException
   */
  @Override
  public void cancel() throws SQLException {
    throw new SQLFeatureNotSupportedException();
//    if (isCancelled || instance == null) {
//      return;
//    }
//
//    try {
//      instance.stop();
//    } catch (OdpsException e) {
//      throw new SQLException("cancel error", e);
//    }
//
//    isCancelled = true;
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /**
   * Each resultSet is associated with a statement. If the same statement is used to
   * execute another query, the original resultSet will be invalidated.
   * And the corresponding temp table will be dropped as well.
   *
   * @throws SQLException
   */
  @Override
  public void close() throws SQLException {
    if (isClosed || resultSet == null) {
      return;
    }

    // Drop the temp table for querying result set and ensure it has been dropped
    Instance dropTableInstance = conn.run(String.format("drop table %s;", tempTableName));
    try {
      if (!dropTableInstance.isSuccessful()) {
        throw new SQLException("can not drop the temp table for querying result");
      }
    } catch (OdpsException e) {
      throw new SQLException("can not read instance status", e);
    }

    resultSet.close();
    resultSet = null;
    isClosed = true;
  }

  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {

    beforeExecute();

    // Drop the temp table for querying result set
    // in case that the table has been created in the former execution.
    conn.run(String.format("drop table if exists %s;", tempTableName));

    // Create a temp table for querying ResultSet and ensure its creation.
    // If the table can not be created, an exception will be caused.
    // Once the table has been created, it will last until the Statement is closed.
    Instance createTableInstance =
        conn.run(String.format("create table %s as %s", tempTableName, sql));
    try {
      if (!createTableInstance.isSuccessful()) {
        throw new SQLException("can not create the temp table for querying result");
      }
    } catch (OdpsException e) {
      throw new SQLException("can not read instance status", e);
    }

    // Read schema
    Table table = conn.getOdps().tables().get(tempTableName);
    List<String> columnNames = new ArrayList<String>();
    List<OdpsType> columnSqlTypes = new ArrayList<OdpsType>();
    try {
      table.reload();
      for (Column col : table.getSchema().getColumns()) {
        columnNames.add(col.getName());
        columnSqlTypes.add(col.getType());
      }
    } catch (OdpsException e) {
      throw new SQLException("can not read table schema", e);
    }
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);

    // Create a download session through tunnel
    TableTunnel tunnel = new TableTunnel(conn.getOdps());
    DownloadSession downloadSession;
    try {
      downloadSession =
          tunnel.createDownloadSession(conn.getOdps().getDefaultProject(), tempTableName);
    } catch (TunnelException e) {
      throw new SQLException("can not create tunnel download session", e);
    }

    // Construct result set if the query
    resultSet = new OdpsQueryResultSet.Builder().setStmtHandle(this).setMeta(meta)
        .setSessionHandle(downloadSession)
        .setFetchForward(isResultSetFetchForward)
        .setScollable(isResultSetScrollable)
        .setFetchSize(fetchSize)
        .setMaxRows(maxRows).build();

    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    beforeExecute();

    instance = conn.run(sql);

    try {
      Instance.TaskSummary taskSummary = instance.getTaskSummary("SQL");
      if (taskSummary != null) {
        JSONObject jsonSummary = JSON.parseObject(taskSummary.getJsonSummary());
        JSONObject outputs = jsonSummary.getJSONObject("Outputs");

        if (outputs.size() > 0) {
          updateCount = 0;
          for (Object item : outputs.values()) {
            JSONArray array = (JSONArray) item;
            updateCount += array.getInteger(0);
          }
        }
      }
    } catch (OdpsException e) {
      throw new SQLException("get task summary error", e);
    }

    return updateCount;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    BufferedReader reader = new BufferedReader(new StringReader(sql));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.matches("^\\s*(--|#)")) {  // skip the comment starting with '--' or '#'
          continue;
        }
        if (line.matches("(?i)^(\\s*)(SELECT).*$")) {
          executeQuery(sql);
          return true;
        }
      }
    } catch (IOException e) {
      throw new SQLException("can not read sql: ", e);
    }

    executeUpdate(sql);
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public OdpsConnection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();
    return isResultSetFetchForward ? ResultSet.FETCH_FORWARD : ResultSet.FETCH_REVERSE;
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkClosed();
    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    fetchSize = rows;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max must be >= 0");
    }
    this.maxRows = max;
  }

  /**
   * This implementation do not support multiple open results.
   *
   * @return whether has more results
   * @throws SQLException
   */
  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // TODO
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Not supported direction: " + direction);
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  private void beforeExecute() {
    instance = null;
    updateCount = -1;
    resultSet = null;
    isCancelled = false;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("The statement has been closed");
    }
  }
}
