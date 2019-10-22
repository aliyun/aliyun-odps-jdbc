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

import com.aliyun.odps.Project;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.MDC;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.jdbc.utils.LoggerFactory;
import com.aliyun.odps.jdbc.utils.Utils;

public class OdpsConnection extends WrapperAdapter implements Connection {

  private final Odps odps;
  private final Properties info;
  private final List<Statement> stmtHandles;

  /**
   * For each connection, keep a character set label for layout the ODPS's byte[] storage
   */
  private final String charset;

  private final String logviewHost;

  /**
   * The lifecycle of the temp table created when execute query
   */
  protected final int lifecycle;

  private boolean isClosed = false;

  /**
   * Per-connection logger. All its statements produced by this connection will share this logger
   */
  protected OdpsLogger log;

  private SQLWarning warningChain = null;

  private String connectionId;
  
  private final Properties sqlTaskProperties = new Properties();
  
  private String tunnelEndpoint;

  OdpsConnection(String url, Properties info) throws SQLException {

    ConnectionResource connRes = new ConnectionResource(url, info);
    String accessId = connRes.getAccessId();
    String accessKey = connRes.getAccessKey();
    String charset = connRes.getCharset();
    String project = connRes.getProject();
    String endpoint = connRes.getEndpoint();
    String tunnelEndpoint = connRes.getTunnelEndpoint();
    String logviewHost = connRes.getLogview();
    String logConfFile = connRes.getLogConfFile();

    int lifecycle;
    try {
      lifecycle = Integer.parseInt(connRes.getLifecycle());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("lifecycle is expected to be an integer");
    }

    connectionId = UUID.randomUUID().toString().substring(24);
    MDC.put("connectionId", connectionId);

    log = new OdpsLogger(getClass().getName(), null, false, logConfFile);

    if (connRes.getLogLevel() != null) {
      log.warn("The logLevel is deprecated, please set log level in log conf file!");
    }

    String version = Utils.retrieveVersion("driver.version");
    log.info("ODPS JDBC driver, Version " + version);
    log.info(String.format("endpoint=%s, project=%s", endpoint, project));
    log.info("JVM timezone : " + TimeZone.getDefault().getID());
    log.debug(String
        .format("charset=%s, logview=%s, lifecycle=%d", charset, logviewHost, lifecycle));

    Account account = new AliyunAccount(accessId, accessKey);
    log.debug("debug mode on");
    odps = new Odps(account);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    odps.setUserAgent("odps-jdbc-" + version);

    this.info = info;
    this.charset = charset;
    this.logviewHost = logviewHost;
    this.lifecycle = lifecycle;
    this.tunnelEndpoint = tunnelEndpoint;
    this.stmtHandles = new ArrayList<Statement>();

    try {
      odps.projects().get().reload();
      String msg = "Connect to odps project %s successfully";
      log.debug(String.format(msg, odps.getDefaultProject()));
    } catch (OdpsException e) {
      throw new SQLException(e);
    }
  }



  @Override
  public OdpsPreparedStatement prepareStatement(String sql) throws SQLException {
    OdpsPreparedStatement stmt = new OdpsPreparedStatement(this, sql);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Only support the following type
   *
   * @param sql the prepared sql
   * @param resultSetType TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
   * @param resultSetConcurrency CONCUR_READ_ONLY
   * @return OdpsPreparedStatement
   * @throws SQLException wrong type
   */
  @Override
  public OdpsPreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    checkClosed();

    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLFeatureNotSupportedException("Statement with resultset type: " + resultSetType
          + " is not supported");
    }

    if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
      throw new SQLFeatureNotSupportedException("Statement with resultset concurrency: "
          + resultSetConcurrency + " is not supported");
    }

    boolean isResultSetScrollable = (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
    OdpsPreparedStatement stmt = new OdpsPreparedStatement(this, sql, isResultSetScrollable);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (!autoCommit) {
      log.error(Thread.currentThread().getStackTrace()[1].getMethodName()
          + " to false is not supported!!!");
      throw new SQLFeatureNotSupportedException("enabling autocommit is not supported");
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public void commit() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException {
    MDC.remove("connectionId");
    if (!isClosed) {
      for (Statement stmt : stmtHandles) {
        if (stmt != null) {
          stmt.close();
        }
      }
    }
    isClosed = true;
    log.info("connection closed");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    return new OdpsDatabaseMetaData(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (readOnly) {
      log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
      throw new SQLFeatureNotSupportedException("enabling read-only is not supported");
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  /**
   * ODPS doesn't support the concept of catalog Each connection is associated with one endpoint
   * (embedded in the connection url). Each endpoint has a couple of projects (schema)
   */
  @Override
  public void setCatalog(String catalog) throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public OdpsStatement createStatement() throws SQLException {
    checkClosed();
    OdpsStatement stmt = new OdpsStatement(this, false);
    stmtHandles.add(stmt);
    return stmt;
  }

  /**
   * Only support the following type:
   *
   * @param resultSetType TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
   * @param resultSetConcurrency CONCUR_READ_ONLY
   * @return OdpsStatement object
   * @throws SQLException wrong type
   */
  @Override
  public OdpsStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();

    boolean isResultSetScrollable;

    switch (resultSetType) {
      case ResultSet.TYPE_FORWARD_ONLY:
        isResultSetScrollable = false;
        break;
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        isResultSetScrollable = true;
        break;
      default:
        throw new SQLFeatureNotSupportedException(
            "only support statement with ResultSet type: TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_FORWARD_ONLY");
    }

    switch (resultSetConcurrency) {
      case ResultSet.CONCUR_READ_ONLY:
        break;
      default:
        throw new SQLFeatureNotSupportedException(
            "only support statement with ResultSet concurrency: CONCUR_READ_ONLY");
    }

    OdpsStatement stmt = new OdpsStatement(this, isResultSetScrollable);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("timeout value was negative");
    }
    try {
      odps.projects().exists("123");
      return true;
    } catch (OdpsException e) {
      return false;
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    this.info.putAll(properties);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    this.info.put(name, value);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return info;
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return info.getProperty(name);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public void setSchema(String schema) throws SQLException {
    checkClosed();
    odps.setDefaultProject(schema);
  }

  public String getSchema() throws SQLException {
    checkClosed();
    return odps.getDefaultProject();
  }

  public void abort(Executor executor) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public int getNetworkTimeout() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public Odps getOdps() {
    return this.odps;
  }

  private void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("the connection has already been closed");
    }
  }

  protected String getCharset() {
    return charset;
  }

  protected String getLogviewHost() {
    return logviewHost;
  }

  public Properties getSqlTaskProperties() {
    return sqlTaskProperties;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }
      
}
