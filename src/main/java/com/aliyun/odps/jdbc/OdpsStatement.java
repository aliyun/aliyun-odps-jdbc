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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.Status;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.utils.SqlParserUtil;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.InstanceTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

public class OdpsStatement extends WrapperAdapter implements Statement {

  private OdpsConnection connHandle;
  private Instance executeInstance = null;
  private ResultSet resultSet = null;
  private int updateCount = -1;
  private int queryTimeout = -1;

  // result cache in session mode
  com.aliyun.odps.data.ResultSet sessionResultSet = null;

  // when the update count is fetched by the client, set this true
  // Then the next call the getUpdateCount() will return -1, indicating there's no more results.
  // see Issue #15
  boolean updateCountFetched = false;

  private boolean isClosed = false;
  private boolean isCancelled = false;

  private static final int POLLING_INTERVAL = 3000;
  private static final String JDBC_SQL_TASK_NAME = "jdbc_sql_task";
  private static ResultSet EMPTY_RESULT_SET = null;
  static {
    try {
      OdpsResultSetMetaData meta =
          new OdpsResultSetMetaData(Collections.singletonList("N/A"),
                                    Collections.singletonList(TypeInfoFactory.STRING));
      EMPTY_RESULT_SET = new OdpsStaticResultSet(null, meta, null);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

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

  //Unit: result record row count, only applied in interactive mode
  protected Long resultCountLimit = null;
  //Unit: Bytes, only applied in interactive mode
  protected Long resultSizeLimit = null;

  protected boolean enableLimit = false;

  private SQLWarning warningChain = null;

  OdpsStatement(OdpsConnection conn) {
    this(conn, false);
  }

  OdpsStatement(OdpsConnection conn, boolean isResultSetScrollable) {
    this.connHandle = conn;
    sqlTaskProperties = (Properties) conn.getSqlTaskProperties().clone();
    this.resultCountLimit = conn.getCountLimit();
    this.resultSizeLimit = conn.getSizeLimit();
    this.enableLimit = conn.enableLimit();
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
      if (connHandle.runningInInteractiveMode()) {
        connHandle.getExecutor().cancel();
        connHandle.log.info("submit cancel query instance id=" + executeInstance.getId());
      } else {
        // If the instance has already terminated, calling Instance.stop# results in an exception.
        // Checking the instance status before calling Instance.stop# could handle most cases. But
        // if the instance terminated after the checking, an exception would still be thrown.
        if (!executeInstance.isTerminated()) {
          executeInstance.stop();
          connHandle.log.info("submit cancel to instance id=" + executeInstance.getId());
        }
      }
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

    connHandle.log.info("the statement has been closed");

    connHandle = null;
    executeInstance = null;
    sessionResultSet = null;
    isClosed = true;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public synchronized ResultSet executeQuery(String sql) throws SQLException {
    Properties properties = new Properties();
    String query = Utils.parseSetting(sql, properties);
    if (StringUtils.isNullOrEmpty(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return EMPTY_RESULT_SET;
    }
    // otherwise those properties is just for this query

    if (processUseClause(query)) {
      return EMPTY_RESULT_SET;
    }
    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return hasResultSet(query) ? getResultSet() : EMPTY_RESULT_SET;
  }

  @Override
  public synchronized int executeUpdate(String sql) throws SQLException {

    Properties properties = new Properties();

    String query = Utils.parseSetting(sql, properties);

    if (StringUtils.isNullOrEmpty(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return 0;
    }
    // otherwise those properties is just for this query

    if (connHandle.runningInInteractiveMode()) {
      throw new SQLFeatureNotSupportedException("executeUpdate() is not supported in session mode.");
    }
    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return updateCount >= 0 ? updateCount : 0;
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
    Properties properties = new Properties();

    String query = Utils.parseSetting(sql, properties);
    if (StringUtils.isNullOrEmpty(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return false;
    }
    // otherwise those properties is just for this query

    if (processUseClause(query)) {
      return false;
    }

    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return hasResultSet(query);
  }

  public boolean hasResultSet(String sql) throws SQLException {
    if (connHandle.runningInInteractiveMode()) {
      return true;
    }

    if (updateCount == 0) {
      return isQuery(sql) || connHandle.getExecutor().hasResultSet();
    } else {
      return updateCount < 0;
    }
  }

  /**
   * 采用的是odps-sql的语法文件，用来判断sql是否具有结果集。
   *
   * @param sql sql statement
   * @return if the input sql statement is a query statement
   */
  public static boolean isQuery(String sql) {
    return SqlParserUtil.hasResultSet(sql);
  }

  private void processSetClause(Properties properties) {
    for (String key : properties.stringPropertyNames()) {
      connHandle.log.info("set sql task property: " + key + "=" + properties.getProperty(key));
      if (!connHandle.disableConnSetting()) {
        connHandle.getSqlTaskProperties().setProperty(key, properties.getProperty(key));
      }
      sqlTaskProperties.setProperty(key, properties.getProperty(key));
    }
  }

  private boolean processUseClause(String sql) throws SQLFeatureNotSupportedException {
    if (sql.matches("(?i)^(\\s*)(USE)(\\s+)(.*);?(\\s*)$")) {
      if (sql.contains(";")) {
        sql = sql.replace(';', ' ');
      }
      int i = sql.toLowerCase().indexOf("use");
      String project = sql.substring(i + 3).trim();
      if (project.length() > 0) {
        if (connHandle.runningInInteractiveMode()) {
          throw new SQLFeatureNotSupportedException(
              "ODPS-1850001 - 'use project' is not supported in odps jdbc for now.");
        }
        connHandle.getOdps().setDefaultProject(project);
        connHandle.log.info("set project to " + project);
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
    if (!connHandle.runningInInteractiveMode()) {
      throw new SQLFeatureNotSupportedException();
    } else {
      return queryTimeout;
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds <= 0) {
      throw new IllegalArgumentException("Invalid query timeout:" + String.valueOf(seconds));
    }
    if (!connHandle.runningInInteractiveMode()) {
      connHandle.log.error("OdpsDriver do not support query timeout, setQueryTimeout: " + seconds);
    } else {
      queryTimeout = seconds;
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    long startTime = System.currentTimeMillis();
    if (resultSet == null || resultSet.isClosed()) {

      try {
        SQLExecutor sqlExecutor = connHandle.getExecutor();
        com.aliyun.odps.data.ResultSet dataResultSet = sqlExecutor
            .getResultSet(0L, resultCountLimit, resultSizeLimit, enableLimit);
        // 只有command-API匹配的结果才可以进行一次转换
        if (!dataResultSet.isScrollable()) {
          OdpsResultSetMetaData meta = getResultMeta(dataResultSet.getTableSchema().getColumns());
          List<Object[]> res = new ArrayList<>();
          for (Record record : dataResultSet) {
            res.add(record.toArray());
          }
          return new OdpsStaticResultSet(connHandle, meta, res.iterator());
        }
      } catch (OdpsException | IOException ignored) {

      }

      DownloadSession session;
      InstanceTunnel tunnel = new InstanceTunnel(connHandle.getOdps());
      String te = connHandle.getTunnelEndpoint();
      if (!StringUtils.isNullOrEmpty(te)) {
        connHandle.log.info("using tunnel endpoint: " + te);
        tunnel.setEndpoint(te);
      }
      if (!connHandle.runningInInteractiveMode()) {
        // Create a download session through tunnel
        try {
          try {
            session =
                tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId());
          } catch (TunnelException e1) {
            connHandle.log.error("create download session failed: " + e1.getMessage());
            connHandle.log.error("fallback to limit mode");
            session =
                tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId(), true);
          }

          connHandle.log.debug("create download session id=" + session.getId());
          resultSet =
              isResultSetScrollable ? new OdpsScollResultSet(this, getResultMeta(session.getSchema().getColumns()), session,
                                                             OdpsScollResultSet.ResultMode.OFFLINE)
                                    : new OdpsForwardResultSet(this, getResultMeta(session.getSchema().getColumns()), session, startTime);
        } catch (TunnelException e) {
          connHandle.log.error("create download session for session failed: " + e.getMessage());
          e.printStackTrace();

          if ("InstanceTypeNotSupported".equalsIgnoreCase(e.getErrorCode())) {
            return null;
          } else {
            throw new SQLException("create download session failed: instance id="
                                   + executeInstance.getId() + ", Error:" + e.getMessage(), e);
          }
        } catch (IOException e) {
          connHandle.log.error("create download session for session failed: " + e.getMessage());
          e.printStackTrace();
          throw new SQLException("create download session failed: instance id="
                                 + executeInstance.getId() + ", Error:" + e.getMessage(), e);
        }
      } else {
        if (sessionResultSet != null) {
          try {
            OdpsResultSetMetaData meta = getResultMeta(
                sessionResultSet.getTableSchema().getColumns());
            if (isResultSetScrollable && sessionResultSet.isScrollable()) {
              session = tunnel.createDirectDownloadSession(
                  connHandle.getOdps().getDefaultProject(),
                  connHandle.getExecutor().getInstance().getId(),
                  connHandle.getExecutor().getTaskName(),
                  connHandle.getExecutor().getSubqueryId(),
                  enableLimit);
              resultSet = new OdpsScollResultSet(this, meta, session,
                  OdpsScollResultSet.ResultMode.INTERACTIVE);
            } else {
              resultSet = new OdpsSessionForwardResultSet(this, meta, sessionResultSet, startTime);
            }
            sessionResultSet = null;
          } catch (TunnelException e) {
            connHandle.log.error("create download session for session failed: " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("create session resultset failed: instance id="
                                   + connHandle.getExecutor().getInstance().getId() + ", Error:" + e.getMessage(), e);
          } catch (IOException e) {
            connHandle.log.error("create download session for session failed: " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("create session resultset failed: instance id="
                                   + connHandle.getExecutor().getInstance().getId() + ", Error:" + e.getMessage(), e);
          }
        }
      }
    }

    return resultSet;
  }

  private OdpsResultSetMetaData getResultMeta(List<Column> columns) {
    // Read schema
    List<String> columnNames = new ArrayList<String>();
    List<TypeInfo> columnSqlTypes = new ArrayList<TypeInfo>();
    for (Column col : columns) {
      columnNames.add(col.getName());
      columnSqlTypes.add(col.getTypeInfo());
    }
    return new OdpsResultSetMetaData(columnNames, columnSqlTypes);
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
    if (updateCountFetched) {
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

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
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

  @Override
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
    sessionResultSet = null;
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

  private void runSQLOffline(String sql, Map<String, String> settings)
      throws SQLException, OdpsException {

    long begin = System.currentTimeMillis();

    SQLExecutor sqlExecutor = connHandle.getExecutor();
    sqlExecutor.run(sql, settings);
    executeInstance = sqlExecutor.getInstance();
    connHandle.log.info("Run SQL: " + sql);

    if (executeInstance == null) {
      long end = System.currentTimeMillis();
      connHandle.log.info("It took me " + (end - begin) + " ms to run sql");
    } else {
      String logViewUrl = sqlExecutor.getLogView();
      connHandle.log.info(logViewUrl);
      warningChain = new SQLWarning(logViewUrl);

    // Poll the task status within the instance
    boolean complete = false;

    // 等待 instance 结束
    while (!complete) {
      try {
        Thread.sleep(POLLING_INTERVAL);
      } catch (InterruptedException e) {
        break;
      }
      complete = Status.TERMINATED.equals(executeInstance.getStatus());
    }

      String taskName = executeInstance.getTaskNames().iterator().next();
      TaskStatus taskStatus = executeInstance.getTaskStatus().get(taskName);
      if (taskStatus == null) {
        connHandle.log.warn("Failed to get task status. "
            + "The instance may have been killed before its task was created.");
      } else {
        switch (taskStatus.getStatus()) {
          case SUCCESS:
            connHandle.log.debug("sql status: success");
            break;
          case FAILED:
            try {
              String reason = executeInstance.getTaskResults().get(taskName);
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
            connHandle.log.debug("sql status: " + taskStatus.getStatus());
            break;
          default:
        }
      }
      long end = System.currentTimeMillis();
      connHandle.log.info("It took me " + (end - begin) + " ms to run sql");

      // extract update count
      Instance.TaskSummary taskSummary = null;
      try {
        taskSummary = executeInstance.getTaskSummary(taskName);
      } catch (OdpsException e) {
        // update count become uncertain here
        connHandle.log.warn(
            "Failed to get TaskSummary: instance_id=" + executeInstance.getId() + ", taskname="
                + taskName);
      }

      if (taskSummary != null) {
        updateCount = Utils.getSinkCountFromTaskSummary(
            StringEscapeUtils.unescapeJava(taskSummary.getJsonSummary()));
      } else {
        connHandle.log.warn("task summary is empty");
      }
      connHandle.log.debug("successfully updated " + updateCount + " records");
    }

  }

  private void runSQLInSession(String sql, Map<String, String> settings)
      throws SQLException, OdpsException {
    long begin = System.currentTimeMillis();
    SQLExecutor executor = connHandle.getExecutor();
    if (queryTimeout != -1 && !settings.containsKey("odps.sql.session.query.timeout")) {
      settings.put("odps.sql.session.query.timeout", String.valueOf(queryTimeout));
    }
    Long autoSelectLimit = connHandle.getAutoSelectLimit();
    if (autoSelectLimit != null && autoSelectLimit > 0) {
      settings.put("odps.sql.select.auto.limit", autoSelectLimit.toString());
    }
    executor.run(sql, settings);
    try {
      sessionResultSet = executor.getResultSet(0L, resultCountLimit, resultSizeLimit, enableLimit);
      List<String> exeLog = executor.getExecutionLog();
      if (!exeLog.isEmpty()) {
        for (String log : exeLog) {
          connHandle.log.warn("Session execution log:" + log);
        }
      }
    } catch (IOException e) {
      connHandle.log.error("Run SQL failed", e);
      throw new SQLException("execute sql [" + sql + "] instance:["
          + executor.getInstance().getId() + "] failed: " + e.getMessage(), e);
    } catch (OdpsException e) {
      connHandle.log.error("Run SQL failed", e);
      throw new SQLException("execute sql [" + sql + "] instance:["
          + executor.getInstance().getId() + "] failed: " + e.getMessage(), e);
    }
    long end = System.currentTimeMillis();
    connHandle.log.info("It took me " + (end - begin) + " ms to run sql");

    executeInstance = executor.getInstance();

    String logView = executor.getLogView();
    connHandle.log.info("Run SQL: " + sql + ", LogView:" + logView);
    warningChain = new SQLWarning(executor.getSummary());
  }

  private void runSQL(String sql, Properties properties) throws SQLException {
    try {

      // If the client forget to end with a semi-colon, append it.
      if (!sql.endsWith(";")) {
        sql += ";";
      }

      Map<String, String> settings = new HashMap<>();
      for (String key : sqlTaskProperties.stringPropertyNames()) {
        settings.put(key, sqlTaskProperties.getProperty(key));
      }
      if (properties != null && !properties.isEmpty()) {
        for (String key : properties.stringPropertyNames()) {
          settings.put(key, properties.getProperty(key));
        }
      }
      if (!settings.isEmpty()) {
        connHandle.log.info("Enabled SQL task properties: " + settings);
      }

      if (!connHandle.runningInInteractiveMode()) {
        runSQLOffline(sql, settings);
      } else {
        runSQLInSession(sql, settings);
      }

    } catch (OdpsException e) {
      connHandle.log.error("Fail to run sql: " + sql, e);
      throw new SQLException("Fail to run sql:" + sql + ", Error:" + e.toString(), e);
    }
  }

  public Instance getExecuteInstance() {
    return executeInstance;
  }

  public static String getDefaultTaskName() {
    return JDBC_SQL_TASK_NAME;
  }

  public Properties getSqlTaskProperties() {
    return sqlTaskProperties;
  }
}
