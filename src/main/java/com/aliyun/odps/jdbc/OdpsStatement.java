/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.InstanceTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;

public class OdpsStatement extends WrapperAdapter implements Statement {

  private OdpsConnection connHandle;
  private Instance executeInstance = null;
  private ResultSet resultSet = null;
  private int updateCount = -1;

  // when the update count is fetched by the client, set this true
  // Then the next call the getUpdateCount() will return -1, indicating there's no more results.
  // see Issue #15
  boolean updateCountFetched = false;

  private boolean isClosed = false;
  private boolean isCancelled = false;

  private static final int POLLING_INTERVAL = 3000;
  private static final String JDBC_SQL_TASK_NAME = "jdbc_sql_task";
  private static final String SETTINGS = "settings";

  /**
   * The attributes of result set produced by this statement
   */
  protected boolean isResultSetScrollable = false;
  
  private Properties sqlTaskProperties;

  /**
   * The suggestion of fetch direction which might be ignored by the resultSet generated
   */
  enum FetchDirection {
    FORWARD, REVERSE, UNKNOWN
  }

  protected FetchDirection resultSetFetchDirection = FetchDirection.UNKNOWN;

  protected int resultSetMaxRows = 0;
  protected int resultSetFetchSize = 10000;

  private SQLWarning warningChain = null;

  OdpsStatement(OdpsConnection conn) {
    this(conn, false);
  }

  OdpsStatement(OdpsConnection conn, boolean isResultSetScrollable) {
    this.connHandle = conn;
    sqlTaskProperties = (Properties) conn.getSqlTaskProperties().clone();
    this.isResultSetScrollable = isResultSetScrollable;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancel() throws SQLException {
    checkClosed();
    if (isCancelled || executeInstance == null) {
      return;
    }

    try {
      executeInstance.stop();
      connHandle.log.debug("submit cancel to instance id=" + executeInstance.getId());
    } catch (OdpsException e) {
      throw new SQLException(e);
    }

    isCancelled = true;
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }

    connHandle.log.debug("the statement has been closed");

    connHandle = null;
    executeInstance = null;
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
  public synchronized ResultSet executeQuery(String sql) throws SQLException {
    
    checkClosed();
    beforeExecute();
    runSQL(sql);

    // Create a download session through tunnel
    DownloadSession session;
    try {
      InstanceTunnel tunnel = new InstanceTunnel(connHandle.getOdps());
      String te = connHandle.getTunnelEndpoint();
      if (!StringUtils.isNullOrEmpty(te)) {
        connHandle.log.info("using tunnel endpoint: " + te);
        tunnel.setEndpoint(te);
      }
      try {
        session =
            tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                                         executeInstance.getId());
      } catch (TunnelException e1) {
        connHandle.log.info("create download session failed: " + e1.getMessage());
        connHandle.log.info("fallback to limit mode");
        session =
            tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                                         executeInstance.getId(), true);
      }

      connHandle.log.info("create download session id=" + session.getId());
    } catch (TunnelException e) {
      throw new SQLException("create download session failed: instance id="
                             + executeInstance.getId(), e);
    }

    // Read schema
    List<String> columnNames = new ArrayList<String>();
    List<TypeInfo> columnSqlTypes = new ArrayList<TypeInfo>();
    for (Column col : session.getSchema().getColumns()) {
      columnNames.add(col.getName());
      columnSqlTypes.add(col.getTypeInfo());
    }
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);

    resultSet =
        isResultSetScrollable ? new OdpsScollResultSet(this, meta, session)
            : new OdpsForwardResultSet(this, meta, session);

    return resultSet;
  }

  @Override
  public synchronized int executeUpdate(String sql) throws SQLException {
    
    int returnRowCount = 0;
    
    checkClosed();
    beforeExecute();
    runSQL(sql);

    // extract update count
    Instance.TaskSummary taskSummary;
    try {
      taskSummary = executeInstance.getTaskSummary(JDBC_SQL_TASK_NAME);
    } catch (OdpsException e) {
      throw new SQLException("Fail to get the task summary", e);
    }

    if (taskSummary != null) {
      JSONObject jsonSummary = JSON.parseObject(taskSummary.getJsonSummary());
      JSONObject outputs = jsonSummary.getJSONObject("Outputs");

      if (outputs.size() > 0) {
        for (Object item : outputs.values()) {
          JSONArray array = (JSONArray) item;
          returnRowCount += array.getInteger(0);
        }
        updateCount = returnRowCount;
      }
    } else {
      connHandle.log.warn("task summary is empty");
    }
    
    connHandle.log.info("successfully updated " + returnRowCount + " records");
    
    return returnRowCount;
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
    // short cut for SET clause
    if (processSetClause(sql)) {
      return false;
    }
    
    if (processUseClause(sql)) {
      return false;
    }

    if (isQuery(sql)) {
      executeQuery(sql);
      return true;
    }
    executeUpdate(sql);
    return false;
  }

  /**
   *
   * @param sql sql statement
   * @return if the input sql statement is a query statement
   * @throws SQLException
   */
  public static boolean isQuery(String sql) throws SQLException {
    BufferedReader reader = new BufferedReader(new StringReader(sql));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.matches("^\\s*(--|#).*")) { // skip the comment starting with '--' or '#'
          continue;
        }
        if (line.matches("^\\s*$")) { // skip the whitespace line
          continue;
        }
        // The first none-comment line start with "select"
        if (line.matches("(?i)^(\\s*)(SELECT).*$")) {
          return true;
        } else {
          break;
        }
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
    return false;
  }

  private boolean processSetClause(String sql) {
    if (sql.matches("(?i)^(\\s*)(SET)(\\s+)(.*)=(.*);?(\\s*)$")) {
      if (sql.contains(";")) {
        sql = sql.replace(';', ' ');
      }
      int i = sql.toLowerCase().indexOf("set");
      String pairstring = sql.substring(i + 3);
      String[] pair = pairstring.split("=");
      connHandle.log.debug("set sql task property: " + pair[0].trim() + "=" + pair[1].trim());
      connHandle.getSqlTaskProperties().setProperty(pair[0].trim(), pair[1].trim());
      sqlTaskProperties.setProperty(pair[0].trim(), pair[1].trim());
      return true;
    }
    return false;
  }
  
  private boolean processUseClause(String sql) {
    if (sql.matches("(?i)^(\\s*)(USE)(\\s+)(.*);?(\\s*)$")) {
      if (sql.contains(";")) {
        sql = sql.replace(';', ' ');
      }
      int i = sql.toLowerCase().indexOf("use");
      String project = sql.substring(i + 3).trim();
      if (project.length() > 0) {
        connHandle.getOdps().setDefaultProject(project);
        connHandle.log.debug("set project to " + project);
      }
      return true;
    }
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
    return connHandle;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkClosed();

    int direction;
    switch (resultSetFetchDirection) {
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
    checkClosed();
    return resultSetFetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    resultSetFetchSize = rows;
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
    return resultSetMaxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max must be >= 0");
    }
    this.resultSetMaxRows = max;
  }

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
  public synchronized int getUpdateCount() throws SQLException {
    checkClosed();
    if (updateCountFetched){
      return -1;
    }
    updateCountFetched = true;
    if (executeInstance == null) {
      return -1;
    }
    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

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

  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

    switch (direction) {
      case ResultSet.FETCH_FORWARD:
        resultSetFetchDirection = FetchDirection.FORWARD;
        break;
      case ResultSet.FETCH_REVERSE:
        resultSetFetchDirection = FetchDirection.REVERSE;
        break;
      case ResultSet.FETCH_UNKNOWN:
        resultSetFetchDirection = FetchDirection.UNKNOWN;
        break;
      default:
        throw new SQLException("invalid argument for setFetchDirection()");
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  private void beforeExecute() throws SQLException {
    // If the statement re-executes another query, the previously-generated resultSet
    // will be implicit closed. And the corresponding temp table will be dropped as well.
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }

    executeInstance = null;
    isClosed = false;
    isCancelled = false;
    updateCount = -1;
    updateCountFetched = false;
  }

  protected OdpsLogger getParentLogger() {
    return connHandle.log;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("The statement has been closed");
    }
  }

  private void runSQL(String sql) throws SQLException {
    try {

      // If the client forget to end with a semi-colon, append it.
      if (!sql.endsWith(";")) {
        sql += ";";
      }

      long begin = System.currentTimeMillis();

      Odps odps = connHandle.getOdps();
      SQLTask sqlTask = new SQLTask();
      sqlTask.setName(JDBC_SQL_TASK_NAME);
      sqlTask.setQuery(sql);
      Map<String,String> settings = new HashMap<String,String>();
      for (String key : sqlTaskProperties.stringPropertyNames()) {
        settings.put(key, sqlTaskProperties.getProperty(key));
      }
      if (!settings.isEmpty()) {
        sqlTask.setProperty(SETTINGS, JSON.toJSONString(settings));
      }
      if (!sqlTaskProperties.isEmpty()) {
        connHandle.log.debug("Enabled SQL task properties: " + sqlTaskProperties);
      }

      executeInstance = odps.instances().create(sqlTask);

      LogView logView = new LogView(odps);
      if (connHandle.getLogviewHost() != null) {
        logView.setLogViewHost(connHandle.getLogviewHost());
      }
      String logViewUrl = logView.generateLogView(executeInstance, 7 * 24);
      connHandle.log.debug("Run SQL: " + sql);
      connHandle.log.info(logViewUrl);
      warningChain = new SQLWarning(logViewUrl);

      // Poll the task status within the instance
      boolean complete = false;
      Instance.TaskStatus taskstatus;

      while (!complete) {
        try {
          Thread.sleep(POLLING_INTERVAL);
        } catch (InterruptedException e) {
          break;
        }

        try {
          taskstatus = executeInstance.getTaskStatus().get(JDBC_SQL_TASK_NAME);
          if (taskstatus == null) {
            connHandle.log.warn("NullPointer when get task status");
            // NOTE: keng!!
            continue;
          }
        } catch (OdpsException e) {
          connHandle.log.error("Fail to get task status:" + sql, e);
          throw new SQLException("Fail to get task status", e);
        }

        switch (taskstatus.getStatus()) {
          case SUCCESS:
            complete = true;
            connHandle.log.debug("sql status: success");
            break;
          case FAILED:
            try {
              String reason = executeInstance.getTaskResults().get(JDBC_SQL_TASK_NAME);
              connHandle.log.error("execute sql [" + sql + "] failed: " + reason);
              throw new SQLException("execute sql [" + sql + "] failed: " + reason, "FAILED");
            } catch (OdpsException e) {
              connHandle.log.error("Fail to get task status:" + sql, e);
              throw new SQLException("Fail to get task status", e);
            }
          case CANCELLED:
            connHandle.log.info("execute instance cancelled");
            throw new SQLException("execute instance cancelled", "CANCELLED");
          case WAITING:
          case RUNNING:
          case SUSPENDED:
            connHandle.log.debug("sql status: " + taskstatus.getStatus());
            break;
        }
      }

      // 等待 instance 结束
      executeInstance.waitForSuccess(POLLING_INTERVAL);
      long end = System.currentTimeMillis();
      connHandle.log.debug("It took me " + (end - begin) + " ms to run sql");
    } catch (OdpsException e) {
      connHandle.log.error("Fail to run sql: " + sql, e);
      throw new SQLException("Fail to run sql:" + sql, e);
    }
  }

  public Instance getExecuteInstance() {
    return executeInstance;
  }
  
}
