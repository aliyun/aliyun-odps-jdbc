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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.MDC;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.Tenant;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.utils.OdpsConstants;
import com.aliyun.odps.utils.StringUtils;

public class OdpsConnection extends WrapperAdapter implements Connection {

  private static final AtomicLong CONNECTION_ID_GENERATOR = new AtomicLong(0);

  private final Odps odps;
  private final TimeZone tz;
  private final Properties info;
  private final List<Statement> stmtHandles;

  /**
   * For each connection, keep a character set label for layout the ODPS's byte[] storage
   */
  private final String charset;

  private final String logviewHost;

  private boolean isClosed = false;

  /**
   * Per-connection logger. All its statements produced by this connection will share this logger
   */
  protected OdpsLogger log;

  private SQLWarning warningChain = null;

  private String connectionId;

  private final Properties sqlTaskProperties = new Properties();

  private String tunnelEndpoint;

  private String majorVersion;

  private String fallbackQuota;
  private static final String MAJOR_VERSION = "odps.task.major.version";
  private static String ODPS_SETTING_PREFIX = "odps.";
  private boolean interactiveMode = false;
  private Long autoSelectLimit = null;
  private Map<String, Map<String, List<String>>> tables;
  //Unit: result record row count, only applied in interactive mode
  private Long resultCountLimit = null;
  //Unit: Bytes, only applied in interactive mode
  private Long resultSizeLimit = null;
  //Tunnel get result retry time, tunnel will retry every 10s
  private int tunnelRetryTime;
  //Unit: seconds, only applied in interactive mode
  private Long attachTimeout;

  private boolean disableConnSetting = false;

  private boolean useProjectTimeZone = false;

  private boolean enableLimit = false;

  private boolean autoLimitFallback = false;

  private SQLExecutor executor = null;

  private String executeProject = null;

  private CatalogSchema catalogSchema = null;

  public boolean isOdpsNamespaceSchema() {
    return odpsNamespaceSchema;
  }

  private boolean odpsNamespaceSchema = false;

  private int readTimeout = -1;
  private int connectTimeout = -1;

  OdpsConnection(String url, Properties info) throws SQLException {

    ConnectionResource connRes = new ConnectionResource(url, info);
    String accessId = connRes.getAccessId();
    String accessKey = connRes.getAccessKey();
    String charset = connRes.getCharset();
    String project = connRes.getProject();
    String schema = connRes.getSchema();
    String endpoint = connRes.getEndpoint();
    String tunnelEndpoint = connRes.getTunnelEndpoint();
    String logviewHost = connRes.getLogview();
    String logConfFile = connRes.getLogConfFile();
    String serviceName = connRes.getInteractiveServiceName();
    String stsToken = connRes.getStsToken();
    sqlTaskProperties.put(Utils.JDBC_USER_AGENT, Utils.JDBCVersion + " " + Utils.SDKVersion);

    connectionId = Long.toString(CONNECTION_ID_GENERATOR.incrementAndGet());
    MDC.put("connectionId", connectionId);

    int readTimeout;
    try {
      readTimeout = Integer.parseInt(connRes.getReadTimeout());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("read-timeout is expected to be an integer");
    }

    int connectTimeout;
    try {
      connectTimeout = Integer.parseInt(connRes.getConnectTimeout());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("connect-timeout is expected to be an integer");
    }

    log = new OdpsLogger(this.getClass().getName(),
                         connectionId,
                         null,
                         logConfFile,
                         false,
                         connRes.isEnableOdpsLogger());

    String version = Utils.retrieveVersion("driver.version");
    log.info("ODPS JDBC driver, Version " + version);
    log.info(String.format("endpoint=%s, project=%s, schema=%s", endpoint, project, schema));
    log.info("JVM timezone : " + TimeZone.getDefault().getID());
    log.info(String.format("charset=%s, logview host=%s", charset, logviewHost));
    Account account;
    if (stsToken == null || stsToken.length() <= 0) {
      account = new AliyunAccount(accessId, accessKey);
    } else {
      account = new StsAccount(accessId, accessKey, stsToken);
    }
    log.debug("debug mode on");
    odps = new Odps(account);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    odps.setCurrentSchema(schema);
    odps.setUserAgent("odps-jdbc-" + version);

    if (readTimeout > 0) {
      this.readTimeout = readTimeout;
      odps.getRestClient().setReadTimeout(this.readTimeout);
    }

    if (connectTimeout > 0) {
      this.connectTimeout = connectTimeout;
      odps.getRestClient().setConnectTimeout(this.connectTimeout);
    }

    this.info = info;
    this.charset = charset;
    this.logviewHost = logviewHost;
    this.tunnelEndpoint = tunnelEndpoint;
    this.stmtHandles = new ArrayList<>();
    this.sqlTaskProperties.putAll(connRes.getSettings());

    this.tunnelRetryTime = connRes.getTunnelRetryTime();
    this.majorVersion = connRes.getMajorVersion();
    this.interactiveMode = connRes.isInteractiveMode();
    this.tables = Collections.unmodifiableMap(connRes.getTables());
    this.executeProject = connRes.getExecuteProject();
    this.autoSelectLimit = connRes.getAutoSelectLimit();
    this.resultCountLimit = connRes.getCountLimit();
    this.resultSizeLimit = connRes.getSizeLimit();
    this.attachTimeout = connRes.getAttachTimeout();
    this.disableConnSetting = connRes.isDisableConnSetting();
    this.useProjectTimeZone = connRes.isUseProjectTimeZone();
    this.enableLimit = connRes.isEnableLimit();
    this.fallbackQuota = connRes.getFallbackQuota();
    this.autoLimitFallback = connRes.isAutoLimitFallback();

    if (null == connRes.isOdpsNamespaceSchema()) {
      try {
        Tenant tenant = odps.tenant();
        this.odpsNamespaceSchema = Boolean.parseBoolean(tenant.getProperty(OdpsConstants.ODPS_NAMESPACE_SCHEMA));
      } catch (ReloadException e) {
        log.info("tenant doesn't exist, this project cannot support odpsNamespaceSchema.");
        this.odpsNamespaceSchema = false;
      }
    } else {
      this.odpsNamespaceSchema = connRes.isOdpsNamespaceSchema();
    }
    this.catalogSchema = new CatalogSchema(odps, this.odpsNamespaceSchema);

    try {
      long startTime = System.currentTimeMillis();

      // Default value for odps.sql.timezone
      String timeZoneId = "Asia/Shanghai";
      String projectTimeZoneId = odps.projects().get().getProperty("odps.sql.timezone");
      if (!StringUtils.isNullOrEmpty(projectTimeZoneId)) {
        timeZoneId = projectTimeZoneId;
      }

      log.info("Project timezone: " + timeZoneId);
      tz = TimeZone.getTimeZone(timeZoneId);
      if (interactiveMode) {
        long cost = System.currentTimeMillis() - startTime;
        log.info(String.format("load project meta infos time cost=%d", cost));
        initSQLExecutor(serviceName, connRes.getFallbackPolicy());
      }
      String msg = "Connect to odps project %s successfully";
      log.info(String.format(msg, odps.getDefaultProject()));

    } catch (OdpsException e) {
      log.error("Connect to odps failed:" + e.getMessage());
      throw new SQLException(e);
    }
  }

  public void initSQLExecutor(String serviceName, FallbackPolicy fallbackPolicy)
      throws OdpsException {
    // only support major version when attaching a session
    Map<String, String> hints = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(majorVersion)) {
      hints.put(MAJOR_VERSION, majorVersion);
    }
    for (String key : info.stringPropertyNames()) {
      if (key.startsWith(ODPS_SETTING_PREFIX)) {
        hints.put(key, info.getProperty(key));
      }
    }
    SQLExecutorBuilder builder = new SQLExecutorBuilder();
    Odps executeOdps = this.odps;
    if (!StringUtils.isNullOrEmpty(executeProject)) {
      executeOdps = this.odps.clone();
      executeOdps.setDefaultProject(executeProject);
    }
    builder.odps(executeOdps)
        .properties(hints)
        .serviceName(serviceName)
        .fallbackPolicy(fallbackPolicy)
        .enableReattach(true)
        .attachTimeout(attachTimeout)
        .quotaName(fallbackQuota)
        .tunnelEndpoint(tunnelEndpoint)
        .tunnelGetResultMaxRetryTime(tunnelRetryTime)
        .taskName(OdpsStatement.getDefaultTaskName());
    long startTime = System.currentTimeMillis();
    executor = builder.build();
    if (interactiveMode && executor.getInstance() != null) {
      long cost = System.currentTimeMillis() - startTime;
      log.info(String.format(
          "Attach success, instanceId:%s, attach and get tunnel endpoint time cost=%d",
          executor.getInstance().getId(), cost));
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
   * @param sql                  the prepared sql
   * @param resultSetType        TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
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
                                            int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
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
      throw new SQLFeatureNotSupportedException("disabling autocommit is not supported");
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
        if (stmt != null && !stmt.isClosed()) {
          stmt.close();
        }
      }
      if (runningInInteractiveMode()) {
        executor.close();
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
    catalogSchema.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    return catalogSchema.getCatalog();
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
   * @param resultSetType        TYPE_SCROLL_INSENSITIVE or ResultSet.TYPE_FORWARD_ONLY
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
    // connection validation is already done in constructor, always return true here
    return true;
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

  @Override
  public void setSchema(String schema) throws SQLException {
    checkClosed();
    catalogSchema.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    checkClosed();
    return catalogSchema.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    log.error(Thread.currentThread().getStackTrace()[1].getMethodName() + " is not supported!!!");
    throw new SQLFeatureNotSupportedException();
  }

  public Odps getOdps() {
    return this.odps;
  }

  public TimeZone getProjectTimeZone() {
    return tz;
  }

  public boolean isUseProjectTimeZone() {
    return useProjectTimeZone;
  }

  /**
   * For test
   *
   * @param useProjectTimeZone
   */
  public void setUseProjectTimeZone(boolean useProjectTimeZone) {
    this.useProjectTimeZone = useProjectTimeZone;
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

  public SQLExecutor getExecutor() {
    return executor;
  }

  public boolean runningInInteractiveMode() {
    return interactiveMode;
  }

  Map<String, Map<String, List<String>>> getTables() {
    return tables;
  }

  public String getExecuteProject() {
    return executeProject;
  }

  public Long getAutoSelectLimit() {
    return autoSelectLimit;
  }

  public Long getCountLimit() {
    return resultCountLimit;
  }

  public Long getSizeLimit() {
    return resultSizeLimit;
  }

  public boolean disableConnSetting() {
    return disableConnSetting;
  }

  public boolean enableLimit() {
    return enableLimit;
  }

  public boolean isAutoLimitFallback() {
    return autoLimitFallback;
  }

  public void setEnableLimit(boolean enableLimit) {
    this.enableLimit = enableLimit;
  }

  public int getReadTimeout() {
    if (readTimeout == -1) {
      return odps.getRestClient().getReadTimeout();
    }

    return readTimeout;
  }

  public void setReadTimeout(int readTimeout) {
    if (readTimeout <= 0) {
      throw new IllegalArgumentException("read-timeout should be positive.");
    }
    this.readTimeout = readTimeout;
    odps.getRestClient().setReadTimeout(this.readTimeout);
  }

  public int getConnectTimeout() {
    if (connectTimeout == -1) {
      return odps.getRestClient().getConnectTimeout();
    }

    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    if (connectTimeout <= 0) {
      throw new IllegalArgumentException("connect-timeout should be positive.");
    }
    this.connectTimeout = connectTimeout;
    odps.getRestClient().setConnectTimeout(this.connectTimeout);
  }

  /**
   * get/set catalog/schema depends on odpsNamespaceSchema flag
   */
  static class CatalogSchema {

    private Odps odps;
    private boolean twoTier = true;

    CatalogSchema(Odps odps, boolean odpsNamespaceSchema) {
      this.odps = odps;
      this.twoTier = !odpsNamespaceSchema;
    }

    String getCatalog() {
      if (twoTier) {
        return null;
      } else {
        return odps.getDefaultProject();
      }
    }

    String getSchema() {
      if (twoTier) {
        return odps.getDefaultProject();
      } else {
        return odps.getCurrentSchema();
      }
    }

    void setCatalog(String catalog) {
      if (!twoTier) {
        odps.setDefaultProject(catalog);
      }
    }

    void setSchema(String schema) {
      if (twoTier) {
        this.odps.setDefaultProject(schema);
      } else {
        this.odps.setCurrentSchema(schema);
      }
    }

  }
}