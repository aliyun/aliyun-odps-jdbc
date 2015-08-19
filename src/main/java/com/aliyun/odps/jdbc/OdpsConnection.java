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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.security.SecurityManager;

public class OdpsConnection extends WrapperAdapter implements Connection {

  private final Odps odps;
  private final Properties info;
  private final List<Statement> stmtHandles;

  /**
   * For each connection, keep a character set label for layout the ODPS's byte[] storage
   */
  private final String charset;

  private final String logviewHost;

  private boolean isClosed = false;

  private static Log log = LogFactory.getLog(OdpsConnection.class);

  private SQLWarning warningChain = null;

  OdpsConnection(String url, Properties info) {

    ConnectionResource connRes = new ConnectionResource(url, info);

    String accessId = connRes.getAccessId();
    String accessKey = connRes.getAccessKey();
    String charset = connRes.getCharset();
    String defaultProject = connRes.getDefaultProject();
    String endpoint = connRes.getEndpoint();
    String logviewHost = connRes.getLogviewHost();

    log.debug("accessId=" + accessId);
    log.debug("accessKey=" + accessKey);
    log.debug("endpoint=" + endpoint);
    log.debug("defaultProject=" + defaultProject);
    log.debug("charset=" + charset);
    log.debug("logviewHost=" + logviewHost);

    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(defaultProject);

    this.info = info;
    this.charset = charset;
    this.logviewHost = logviewHost;
    this.stmtHandles = new ArrayList<Statement>();

    // TODO
    odps.getRestClient().setRetryTimes(0);
    odps.getRestClient().setReadTimeout(3);
    odps.getRestClient().setConnectTimeout(3);
  }

  @Override
  public OdpsPreparedStatement prepareStatement(String sql) throws SQLException {
    OdpsPreparedStatement stmt = new OdpsPreparedStatement(this, sql);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Only support the following type
   *
   * @param sql
   *     the prepared sql
   * @param resultSetType
   *     TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
   * @param resultSetConcurrency
   *     CONCUR_READ_ONLY
   * @return OdpsPreparedStatement
   * @throws SQLException
   */
  @Override
  public OdpsPreparedStatement prepareStatement(String sql, int resultSetType,
                                                int resultSetConcurrency) throws SQLException {
    checkClosed();

    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLFeatureNotSupportedException(
          "Statement with resultset type: " + resultSetType + " is not supported");
    }

    if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
      throw new SQLFeatureNotSupportedException(
          "Statement with resultset concurrency: " + resultSetConcurrency + " is not supported");
    }

    boolean isResultSetScrollable = (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
    OdpsPreparedStatement stmt = new OdpsPreparedStatement(this, sql, isResultSetScrollable);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLFeatureNotSupportedException("enabling autocommit is not supported");
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException {
    if (!isClosed) {
      for (Statement stmt : stmtHandles) {
        if (stmt != null) {
          stmt.close();
        }
      }
    }
    isClosed = true;
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkClosed();
    odps.setEndpoint(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    checkClosed();
    return odps.getEndpoint();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
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
   * @param resultSetType
   *     TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
   * @param resultSetConcurrency
   *     CONCUR_READ_ONLY
   * @return OdpsStatement object
   * @throws SQLException
   */
  @Override
  public OdpsStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();

    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLFeatureNotSupportedException(
          "Statement with resultset type: " + resultSetType + " is not supported");
    }

    if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
      throw new SQLFeatureNotSupportedException(
          "Statement with resultset concurrency: " + resultSetConcurrency + " is not supported");
    }

    boolean isResultSetScrollable = (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
    OdpsStatement stmt = new OdpsStatement(this, isResultSetScrollable);
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    checkClosed();
    odps.setDefaultProject(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    checkClosed();
    return odps.getDefaultProject();
  }

  public void abort(Executor executor) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public Odps getOdps() {
    return this.odps;
  }

  public String getUrl() {
    return this.odps.getEndpoint();
  }

  public class LogView {

    private static final String POLICY_TYPE = "BEARER";
    private static final String HOST_DEFAULT = "http://webconsole.odps.aliyun-inc.com:8080";
    private String logViewHost = HOST_DEFAULT;

    Odps odps;

    public LogView(Odps odps) {
      this.odps = odps;
      if (odps.getLogViewHost() != null) {
        logViewHost = odps.getLogViewHost();
      }
    }

    public String generateLogView(Instance instance, long hours) throws OdpsException {
      if (StringUtils.isNullOrEmpty(logViewHost)) {
        return "";
      }

      SecurityManager sm = odps.projects().get(instance.getProject()).getSecurityManager();
      String policy = generatePolicy(instance, hours);
      String token = sm.generateAuthorizationToken(policy, POLICY_TYPE);
      String logview = logViewHost + "/logview/?h=" + odps.getEndpoint() + "&p="
                       + instance.getProject() + "&i=" + instance.getId() + "&token=" + token;
      return logview;
    }

    private String generatePolicy(Instance instance, long hours) {
      String policy = "{\n" //
                      + "    \"expires_in_hours\": " + String.valueOf(hours) + ",\n" //
                      + "    \"policy\": {\n" + "        \"Statement\": [{\n"
                      + "            \"Action\": [\"odps:Read\"],\n"
                      + "            \"Effect\": \"Allow\",\n" //
                      + "            \"Resource\": \"acs:odps:*:projects/" + instance.getProject()
                      + "/instances/"
                      + instance.getId() + "\"\n" //
                      + "        }],\n"//
                      + "        \"Version\": \"1\"\n" //
                      + "    }\n" //
                      + "}";
      return policy;
    }
  }

  /**
   * Kick-offer
   *
   * @param sql
   *     sql string
   * @return an intance
   * @throws SQLException
   */
  protected Instance runClientSQL(String sql) throws SQLException {
    Instance instance;
    try {
      Map<String, String> hints = new HashMap<String, String>();
      Map<String, String> aliases = new HashMap<String, String>();

      // If the client forget to end with a semi-colon, append it.
      if (!sql.contains(";")) {
        sql += ";";
      }

      instance = SQLTask.run(odps, odps.getDefaultProject(), sql, "SQL", hints, aliases);
      LogView logView = new LogView(odps);
      String logViewUrl = logView.generateLogView(instance, 7 * 24);
      log.debug("Run SQL: " + sql + " => Log View: " + logViewUrl);
    } catch (OdpsException e) {
      log.fatal("fail to run sql: " + sql);
      throw new SQLException(e);
    }
    return instance;
  }

  /**
   * Blocked SQL runner, do not print any log information
   *
   * @param sql
   *     sql string
   * @throws SQLException
   */
  protected void runSilentSQL(String sql) throws SQLException {
    try {
      SQLTask.run(odps, sql).waitForSuccess();
    } catch (OdpsException e) {
      throw new SQLException(e);
    }
  }

  private void checkClosed() throws SQLException {
    if (isClosed) {
      throw new SQLException("the connection has already been closed");
    }
  }

  protected String getCharacterSet() {
    return charset;
  }
}
