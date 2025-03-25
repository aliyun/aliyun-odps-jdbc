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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.SettingParser;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.InstanceTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

public class OdpsStatement extends WrapperAdapter implements Statement {

  protected OdpsConnection connHandle;
  protected Instance executeInstance = null;
  protected ResultSet resultSet = null;
  protected int updateCount = -1;
  protected int queryTimeout = -1;

  // result cache in session mode
  com.aliyun.odps.data.ResultSet odpsResultSet = null;
  protected String logviewUrl = null;

  // when the update count is fetched by the client, set this true
  // Then the next call the getUpdateCount() will return -1, indicating there's no more results.
  // see Issue #15
  boolean updateCountFetched = false;

  protected boolean isClosed = false;
  protected boolean isCancelled = false;

  protected static final int POLLING_INTERVAL = 3000;
  protected static final String JDBC_SQL_TASK_NAME = "jdbc_sql_task";
  protected static final String JDBC_SQL_OFFLINE_TASK_NAME = "sqlrt_fallback_task";
  protected static ResultSet EMPTY_RESULT_SET = null;

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

  protected Properties sqlTaskProperties;
  protected Properties inputProperties;

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

  protected SQLWarning warningChain = new SQLWarning();

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
      throw new SQLException(e.getMessage(), e);
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
    odpsResultSet = null;
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
  public synchronized ResultSet executeQuery(String query) throws SQLException {
    Properties properties = new Properties();

    if (!connHandle.isSkipSqlCheck()) {
      SettingParser.ParseResult parseResult = SettingParser.parse(query);
      query = parseResult.getRemainingQuery();
      properties.putAll(parseResult.getSettings());
    }
    if (StringUtils.isBlank(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return EMPTY_RESULT_SET;
    } else {
      try {
        processSetClauseExtra(properties);
      } catch (OdpsException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }
    // otherwise those properties is just for this query

    if (processUseClause(query)) {
      return EMPTY_RESULT_SET;
    }
    checkClosed();
    beforeExecute();
    runSQL(query, properties, false);

    return hasResultSet() ? getResultSet() : EMPTY_RESULT_SET;
  }

  @Override
  public synchronized int executeUpdate(String query) throws SQLException {

    Properties properties = new Properties();
    if (!connHandle.isSkipSqlCheck()) {
      SettingParser.ParseResult parseResult = SettingParser.parse(query);
      query = parseResult.getRemainingQuery();
      properties.putAll(parseResult.getSettings());
    }

    if (StringUtils.isBlank(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return 0;
    } else {
      try {
        processSetClauseExtra(properties);
      } catch (OdpsException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }

    checkClosed();
    beforeExecute();
    runSQL(query, properties, true);

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

  /**
   * 执行SQL
   *
   * @param query any SQL statement
   * @return if it has resultSet
   * @throws SQLException
   */
  @Override
  public boolean execute(String query) throws SQLException {
    // short cut for SET clause
    Properties properties = new Properties();

    if (!connHandle.isSkipSqlCheck()) {
      SettingParser.ParseResult parseResult = SettingParser.parse(query);
      query = parseResult.getRemainingQuery();
      properties.putAll(parseResult.getSettings());
    }

    if (StringUtils.isBlank(query)) {
      // only settings, just set properties
      processSetClause(properties);
      return false;
    } else {
      try {
        processSetClauseExtra(properties);
      } catch (OdpsException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }
    // otherwise those properties is just for this query

    if (processUseClause(query)) {
      return false;
    }

    checkClosed();
    beforeExecute();
    runSQL(query, properties);

    return hasResultSet();
  }

  // 内部使用
  protected boolean hasResultSet() {
    if (connHandle.getExecutor() == null) {
      return false;
    }

    if (odpsResultSet != null) {
      return true;
    }

    return connHandle.getExecutor().hasResultSet();
  }

  @Deprecated
  public boolean hasResultSet(String sql) throws SQLException {
    if (connHandle.runningInInteractiveMode()) {
      return true;
    }

    if (updateCount == 0) {
      return isQuery(sql);
    } else {
      return updateCount < 0;
    }
  }

  /**
   * @param sql sql statement
   * @return if the input sql statement is a select statement
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
      throw new SQLException(e.getMessage(), e);
    }
    return false;
  }

  protected void processSetClause(Properties properties) {
    for (String key : properties.stringPropertyNames()) {
      connHandle.log.info("set sql task property: " + key + "=" + properties.getProperty(key));
      if (!connHandle.disableConnSetting()) {
        connHandle.getSqlTaskProperties().setProperty(key, properties.getProperty(key));
      }
      sqlTaskProperties.setProperty(key, properties.getProperty(key));
    }
    try {
      processSetClauseExtra(properties);
    } catch (Exception e) {
      connHandle.log.error("processSetClauseExtra error", e);
    }
  }

  /**
   * This method is actually only used for debugging.
   * If the user wants to change the behavior of jdbc, he should add parameters to the link string instead of adding settings in the code.
   * This method and corresponding functionality may be removed at any time.
   */
  protected void processSetClauseExtra(Properties properties) throws OdpsException {
    for (String key : properties.stringPropertyNames()) {
      connHandle.log.info("set sql task property extra: " + key + "=" + properties.getProperty(key));
      if (key.equalsIgnoreCase("tunnelEndpoint")) {
        connHandle.setTunnelEndpoint(properties.getProperty(key));
      }
      if (key.equalsIgnoreCase("useTunnel")) {
        connHandle.setUseInstanceTunnel(Boolean.parseBoolean(properties.getProperty(key)));
      }
      if (key.equalsIgnoreCase("interactiveMode")) {
        connHandle.setInteractiveMode(Boolean.parseBoolean(properties.getProperty(key)));
      }
    }
  }

  protected boolean processUseClause(String sql) throws SQLFeatureNotSupportedException {
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
    if (resultSet == null || resultSet.isClosed() && odpsResultSet != null) {
        OdpsResultSetMetaData
            meta =
            getResultMeta(odpsResultSet.getTableSchema().getColumns());
        try {
          if (!isResultSetScrollable || connHandle.getExecutor().getInstance() == null) {
            resultSet = new OdpsSessionForwardResultSet(this, meta, odpsResultSet, startTime);
          } else {
            DownloadSession session;
            InstanceTunnel tunnel = new InstanceTunnel(connHandle.getOdps());
            String te = connHandle.getTunnelEndpoint();
            if (!StringUtils.isNullOrEmpty(te)) {
              connHandle.log.info("using tunnel endpoint: " + te);
              tunnel.setEndpoint(te);
            }
            if (connHandle.getTunnelConnectTimeout() >= 0) {
              tunnel.getConfig().setSocketConnectTimeout(connHandle.getTunnelConnectTimeout());
            }
            if (connHandle.getTunnelReadTimeout() >= 0) {
              tunnel.getConfig().setSocketTimeout(connHandle.getTunnelReadTimeout());
            }
            session = tunnel.createDirectDownloadSession(
                connHandle.getOdps().getDefaultProject(),
                connHandle.getExecutor().getInstance().getId(),
                connHandle.getExecutor().getTaskName(),
                connHandle.getExecutor().getSubqueryId(),
                enableLimit);

            resultSet = new OdpsScollResultSet(this, meta, session,
                                               connHandle.getExecutor()
                                                                   .getExecuteMode() == ExecuteMode.INTERACTIVE
                                                               ? OdpsScollResultSet.ResultMode.INTERACTIVE
                                                               : OdpsScollResultSet.ResultMode.OFFLINE);
          }
          odpsResultSet = null;
        } catch (TunnelException e) {
          connHandle.log.error("create download session for session failed: " + e.getMessage());
          e.printStackTrace();
          throw new SQLException("create session resultset failed: instance id="
                                 + connHandle.getExecutor().getInstance().getId() + ", Error:" + e
                                     .getMessage(), e);
        } catch (IOException e) {
          connHandle.log.error("create download session for session failed: " + e.getMessage());
          e.printStackTrace();
          throw new SQLException("create session resultset failed: instance id="
                                 + connHandle.getExecutor().getInstance().getId() + ", Error:" + e
                                     .getMessage(), e);
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

  public ExecuteMode getExecuteMode() {
    return connHandle.getExecutor().getExecuteMode();
  }

  protected void beforeExecute() throws SQLException {
    // If the statement re-executes another query, the previously-generated resultSet
    // will be implicit closed. And the corresponding temp table will be dropped as well.
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }

    executeInstance = null;
    odpsResultSet = null;
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

  protected void throwSQLException(Exception e, String sql, Instance instance, String logviewUrl) throws SQLException {
    connHandle.log.error("LogView: " + logviewUrl);
    connHandle.log.error("Run SQL failed", e);
    throw new SQLException(
        "execute sql [ " + sql + " ] + failed. " + (instance == null ? "" : "instanceId:["
                                                                            + instance.getId()
                                                                            + "]")
        + e.getMessage(), e);
  }

  private void runSQL(String sql, Properties properties) throws SQLException {
    runSQL(sql, properties, false);
  }

  private void runSQL(String sql, Properties properties, boolean isUpdate) throws SQLException {
    SQLExecutor executor = connHandle.getExecutor();
    try {

      // If the client forget to end with a semi-colon, append it.
      if (!sql.endsWith(";")) {
        sql += ";";
      }

      Map<String, String> settings = new HashMap<>();
      for (String key : sqlTaskProperties.stringPropertyNames()) {
        settings.put(key, sqlTaskProperties.getProperty(key));
      }

      inputProperties = new Properties();
      if (properties != null && !properties.isEmpty()) {
        for (String key : properties.stringPropertyNames()) {
          settings.put(key, properties.getProperty(key));
          inputProperties.put(key, properties.getProperty(key));
        }
      }
      if (!settings.isEmpty()) {
        connHandle.log.info("Enabled SQL task properties: " + settings);
      }
      long begin = System.currentTimeMillis();
      if (queryTimeout != -1 && !settings.containsKey("odps.sql.session.query.timeout")) {
        settings.put("odps.sql.session.query.timeout", String.valueOf(queryTimeout));
      }
      Long autoSelectLimit = connHandle.getAutoSelectLimit();
      if (autoSelectLimit != null && autoSelectLimit > 0) {
        settings.put("odps.sql.select.auto.limit", autoSelectLimit.toString());
      }
      connHandle.log.info("Run SQL: " + sql + ", Begin time: " + begin);
      executor.run(sql, settings);
      logviewUrl = executor.getLogView();
      connHandle.log.info("LogView: " + logviewUrl);
      executeInstance = executor.getInstance();
      if (executeInstance != null) {
        connHandle.log.info("InstanceId: " + executeInstance.getId());
      }
      if (isUpdate) {
        if (executeInstance != null) {
          executeInstance.waitForSuccess();
          Instance.TaskSummary taskSummary = null;
          try {
            taskSummary = executeInstance.getTaskSummary(JDBC_SQL_OFFLINE_TASK_NAME);
          } catch (OdpsException e) {
            // update count become uncertain here
            connHandle.log.warn(
                "Failed to get TaskSummary: instance_id=" + executeInstance.getId() + ", taskname="
                + JDBC_SQL_OFFLINE_TASK_NAME);
          }
          if (taskSummary != null) {
            updateCount = Utils.getSinkCountFromTaskSummary(
                StringEscapeUtils.unescapeJava(taskSummary.getJsonSummary()));
            connHandle.log.debug("successfully updated " + updateCount + " records");
          } else {
            connHandle.log.warn("task summary is empty");
          }
        }
        // 如果是DML或者DDL,即使有结果也视为无结果
        odpsResultSet = null;
      } else {
        setResultSetInternal();
      }
      long end = System.currentTimeMillis();
      if (executeInstance != null) {
        connHandle.log.info("It took me " + (end - begin) + " ms to run sql, instanceId: "
                            + executeInstance.getId());
      } else {
        connHandle.log.info("It took me " + (end - begin) + " ms to run sql");
      }
      List<String> exeLog = executor.getExecutionLog();
      if (!exeLog.isEmpty()) {
        for (String log : exeLog) {
          connHandle.log.info("Session execution log: " + log);
        }
      }
    } catch (OdpsException | IOException e) {
      throwSQLException(e, sql, executor.getInstance(), executor.getLogView());
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

  public Properties getInputProperties() {
    return inputProperties;
  }

  public String getLogViewUrl() {
    return logviewUrl;
  }

  protected void setResultSetInternal() throws OdpsException, IOException {
    if (connHandle.isTunnelDownloadUseSingleReader() && getExecuteMode() == ExecuteMode.OFFLINE) {
      connHandle.log.info("Get result by instance tunnel (No page).");
      executeInstance.waitForSuccess();
      Instance instance = executeInstance;

      InstanceTunnel tunnel = new InstanceTunnel(connHandle.getOdps());
      InstanceTunnel.DownloadSession downloadSession = tunnel.createDownloadSession(
          instance.getProject(),
          instance.getId()
      );

      odpsResultSet = new com.aliyun.odps.data.ResultSet(
          new SingleReaderResultSetIterator(downloadSession, downloadSession.getRecordCount()),
          downloadSession.getSchema(),
          downloadSession.getRecordCount());
    } else {
      if (connHandle.getExecutor().isUseInstanceTunnel()) {
        connHandle.log.info("Get result by instance tunnel.");
        odpsResultSet =
            connHandle.getExecutor()
                .getResultSet(0L, resultCountLimit, resultSizeLimit, enableLimit);
      } else {
        connHandle.log.info("Get result by rest api.");
        odpsResultSet = connHandle.getExecutor().getResultSet();
      }
    }
  }

  class SingleReaderResultSetIterator implements Iterator<Record> {

    private final TunnelRecordReader reader;
    private final InstanceTunnel.DownloadSession session;
    private Record nextLine;

    public SingleReaderResultSetIterator(InstanceTunnel.DownloadSession session, long recordCount) {
      try {
        this.session = session;
        this.reader = session.openRecordReader(0, recordCount);
        moveToNextLine();
      } catch (TunnelException | IOException e) {
        throw new RuntimeException("Open tunnel reader failed, session id: " + session.getId()
                                   + " errMsg: " + e.getMessage(),
                                   e);
      }
    }

    @Override
    public boolean hasNext() {
      return nextLine != null;
    }

    @Override
    public Record next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Record currLine = nextLine;
      moveToNextLine();
      return currLine;
    }

    private void moveToNextLine() {
      try {
        nextLine = reader.read();
      } catch (IOException e) {
        nextLine = null;
        throw new RuntimeException("Read record failed, session id: " + session.getId()
                                   + " errMsg: " + e.getMessage(),
                                   e);
      }
    }
  }

}