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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import java.util.*;

import com.aliyun.odps.*;
import com.aliyun.odps.data.SessionQueryResult;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.sun.tools.javac.util.Pair;
import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.task.SQLTask;
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
  private int subQueryId = -1;

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

  private SQLWarning warningChain = null;

  // session query status
  private static int OBJECT_STATUS_RUNNING = 2;
  private static int OBJECT_STATUS_FAILED = 4;

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
      if (connHandle.runningInInteractiveMode()) {
        // TODO cancel session query
        Session session = connHandle.getSessionManager().getSessionInstance();
        session.setInformation("cancel", "*");
        connHandle.log.debug("submit cancel query instance id=" + executeInstance.getId());
      } else {
        executeInstance.stop();
        connHandle.log.debug("submit cancel to instance id=" + executeInstance.getId());
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

  private String preCheckQuery(String sql, Properties properties) {
    String query = "";
    String[] queries = sql.split(";");
    for (String s : queries) {
      Pair<String, String> property = parseSetClause(s);
      if (property == null) {
        query += s;
      } else {
        properties.put(property.fst, property.snd);
      }
    }
    if (StringUtils.isNullOrEmpty(query)) {
      // just set property
      processSetClause(properties);
    }
    return query;
  }

  @Override
  public synchronized ResultSet executeQuery(String sql) throws SQLException {
    Properties properties = new Properties();

    String query = preCheckQuery(sql, properties);

    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return updateCount < 0 ? getResultSet() : EMPTY_RESULT_SET;
  }

  @Override
  public synchronized int executeUpdate(String sql) throws SQLException {
    Properties properties = new Properties();

    String query = preCheckQuery(sql, properties);

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

    String query = preCheckQuery(sql, properties);
    
    if (processUseClause(query)) {
      return false;
    }

    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return updateCount < 0;
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

  private Pair<String, String> parseSetClause(String sql) {
    Pair<String, String> property = null;
    if (sql.matches("(?i)^(\\s*)(SET)(\\s+)(.*)=(.*);?(\\s*)$")) {
      if (sql.contains(";")) {
        sql = sql.replace(';', ' ');
      }
      int i = sql.toLowerCase().indexOf("set");
      String pairstring = sql.substring(i + 3);
      String[] pair = pairstring.split("=");
      connHandle.log.debug("set session property: " + pair[0].trim() + "=" + pair[1].trim());
      property = new Pair<>(pair[0].trim(), pair[1].trim());
    }
    return property;
  }

  private void processSetClause(Properties properties) {
    for (String key : properties.stringPropertyNames()) {
      connHandle.log.debug("set sql task property: " + key + "=" + properties.getProperty(key));
      connHandle.getSqlTaskProperties().setProperty(key, properties.getProperty(key));
      sqlTaskProperties.setProperty(key, properties.getProperty(key));
    }
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
    connHandle.log.debug("OdpsDriver do not support query timeout, setQueryTimeout: " + seconds);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    long startTime = System.currentTimeMillis();
    if (resultSet == null || resultSet.isClosed()) {
      TunnelRecordReader reader = null;
      // Create a download session through tunnel
      DownloadSession session;
      try {
        InstanceTunnel tunnel = new InstanceTunnel(connHandle.getOdps());

        String te = connHandle.getTunnelEndpoint();
        if (!StringUtils.isNullOrEmpty(te)) {
          connHandle.log.debug("using tunnel endpoint: " + te);
          tunnel.setEndpoint(te);
        }
        try {
          if (!connHandle.runningInInteractiveMode()) {
            session =
                tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId());
          } else if (connHandle.isLongPollingSession()) {
            session =
                tunnel.createDirectDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId(), getDefaultTaskName(), subQueryId);
            // in long polling mode, we must open reader to get schema
            reader = session.openRecordReader(0, 1);
          } else {
            session =
                tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId(), getDefaultTaskName(), subQueryId);
          }
        } catch (TunnelException e1) {
          // do not retry when using session
          if (!connHandle.runningInInteractiveMode()) {
            connHandle.log.error("create download session failed: " + e1.getMessage());
            connHandle.log.error("fallback to limit mode");
            session =
                tunnel.createDownloadSession(connHandle.getOdps().getDefaultProject(),
                    executeInstance.getId(), true);
          } else {
            connHandle.log.error("create download session for session failed: " + e1.getMessage());
            throw e1;
          }
        }

        connHandle.log.debug("create download session id=" + session.getId());
      } catch (TunnelException e) {
        throw new SQLException("create download session failed: instance id="
            + executeInstance.getId() + ", Error:" + e.getMessage(), e);
      } catch (IOException e) {
        throw new SQLException("create download session failed: instance id="
            + executeInstance.getId() + ", Error:" + e.getMessage(), e);
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
                                : new OdpsForwardResultSet(this, meta, session, reader, startTime);
    }

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

  private void runSQLOffline(String sql, Odps odps, Map<String,String> settings) throws SQLException, OdpsException {
    long begin = System.currentTimeMillis();
    executeInstance =  SQLTask.run(odps, odps.getDefaultProject(), sql, JDBC_SQL_TASK_NAME, settings, null);
    LogView logView = new LogView(odps);
    if (connHandle.getLogviewHost() != null) {
      logView.setLogViewHost(connHandle.getLogviewHost());
    }
    String logViewUrl = logView.generateLogView(executeInstance, 7 * 24);
    connHandle.log.debug("Run SQL: " + sql);
    connHandle.log.debug(logViewUrl);
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
    connHandle.log.info("It took me " + (end - begin) + " ms to run sql");

    // extract update count
    Instance.TaskSummary taskSummary = null;
    try {
      taskSummary = executeInstance.getTaskSummary(JDBC_SQL_TASK_NAME);
    } catch (OdpsException e) {
      // update count become uncertain here
      connHandle.log.warn("Failed to get TaskSummary: instance_id=" + executeInstance.getId() + ", taskname=" + JDBC_SQL_TASK_NAME);
    }

    if (taskSummary != null) {
      updateCount = Utils.getSinkCountFromTaskSummary(
          StringEscapeUtils.unescapeJava(taskSummary.getJsonSummary()));
    } else {
      connHandle.log.warn("task summary is empty");
    }
    connHandle.log.debug("successfully updated " + updateCount + " records");
  }

  private void runSQLInSessionLongPollingMode(String sql, Map<String,String> settings) throws SQLException, OdpsException {
    long begin = System.currentTimeMillis();
    Session session = connHandle.getSessionManager().getSessionInstance();
    executeInstance = session.getInstance();

    SessionQueryResult subqueryResult =  session.run(sql, settings);
    Session.SubQueryInfo subQueryInfo = subqueryResult.getSubQueryInfo();
    if (subQueryInfo != null) {
      if (StringUtils.isNullOrEmpty(subQueryInfo.result)) {
        subQueryId = subqueryResult.getSubQueryInfo().queryId;
      } else {
        connHandle.log.error("Submit query failed:" + subQueryInfo.result);
        throw new SQLException("Submit query failed:" + subQueryInfo.result);
      }
    } else {
      // will get latest query, never reach here by design
      subQueryId = -1;
    }

    connHandle.log.info("Run SQL: " + sql + ", subQueryId:" + subQueryId);
    connHandle.log.info(session.getLogView());
    warningChain = new SQLWarning(session.getLogView());

    long end = System.currentTimeMillis();
    connHandle.log.info("It took me " + (end - begin) + " ms to submit sql");
  }

  private void runSQLInSession(String sql, Map<String,String> settings) throws SQLException, OdpsException {
    long begin = System.currentTimeMillis();
    Session session = connHandle.getSessionManager().getSessionInstance();
    executeInstance = session.getInstance();

    SessionQueryResult subqueryResult =  session.run(sql, settings);
    Iterator<Session.SubQueryResponse> responseIterator = subqueryResult.getResultIterator();

    connHandle.log.info("Run SQL instance:" + session.getInstance().getId() + " SQL:" + sql);
    connHandle.log.info(session.getLogView());
    warningChain = new SQLWarning(session.getLogView());

    Session.SubQueryResponse response = null;

    // 等待 instance 结束
    while (responseIterator.hasNext()) {
      response = responseIterator.next();
      if (!StringUtils.isNullOrEmpty(response.warnings)) {
        connHandle.log.warn("Warnings: " + response.warnings);
      }
      if (response.status == OBJECT_STATUS_FAILED) {
        connHandle.log.error("Fail to run query:" + response.result);
        throw new SQLException("Fail to run query", response.result);
      } else if (response.status != OBJECT_STATUS_RUNNING) {
        // finished
        connHandle.log.debug("sql status: success");
        break;
      } else {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    }

    long end = System.currentTimeMillis();
    connHandle.log.info("It took me " + (end - begin) + " ms to run sql");
  }

  private void runSQL(String sql, Properties properties) throws SQLException {
    try {

      // If the client forget to end with a semi-colon, append it.
      if (!sql.endsWith(";")) {
        sql += ";";
      }

      Odps odps = connHandle.getOdps();

      Map<String,String> settings = new HashMap<>();
      for (String key : sqlTaskProperties.stringPropertyNames()) {
        settings.put(key, sqlTaskProperties.getProperty(key));
      }
      if (properties != null && !properties.isEmpty()) {
        for (String key : properties.stringPropertyNames()) {
          settings.put(key, properties.getProperty(key));
        }
      }
      if (!settings.isEmpty()) {
        connHandle.log.debug("Enabled SQL task properties: " + sqlTaskProperties);
      }

      if (!connHandle.runningInInteractiveMode()) {
        runSQLOffline(sql, odps, settings);
      } else if (connHandle.isLongPollingSession()) {
        runSQLInSessionLongPollingMode(sql, settings);
      } else {
        runSQLInSession(sql, settings);
      }

    } catch (OdpsException e) {
      connHandle.log.error("Fail to run sql: " + sql, e);
      throw new SQLException("Fail to run sql:" + sql, e);
    }
  }

  public Instance getExecuteInstance() {
    return executeInstance;
  }
  public static String getDefaultTaskName() { return JDBC_SQL_TASK_NAME; }
}
