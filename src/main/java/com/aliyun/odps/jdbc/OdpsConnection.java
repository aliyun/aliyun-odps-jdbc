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
import java.util.logging.Level;

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
import com.aliyun.odps.jdbc.utils.TimeUtils;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.sqa.ExecuteMode;
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

  private int retryTime = -1;
  private int readTimeout = -1;
  private int connectTimeout = -1;

  private boolean enableCommandApi;
  private boolean useInstanceTunnel;
  private boolean httpsCheck;
  private boolean skipSqlCheck;
  private boolean skipSqlInjectCheck;

  private Level logLevel = Level.INFO;

  private int tunnelReadTimeout = -1;
  private int tunnelConnectTimeout = -1;
  private boolean tunnelDownloadUseSingleReader = false;
  private String quotaName;
  private boolean enableMcqaV2 = false;
  private String serviceName = null;
  private FallbackPolicy fallbackPolicy;
  private boolean verbose;
  private int logviewVersion;
  private boolean async;

  private long fetchResultSplitSize;
  private int fetchResultPreloadSplitNum;
  private int fetchResultThreadNum;

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
    logviewVersion = connRes.getLogviewVersion();
    String logConfFile = connRes.getLogConfFile();
    serviceName = connRes.getInteractiveServiceName();
    fallbackPolicy = connRes.getFallbackPolicy();
    String stsToken = connRes.getStsToken();
    String logLevel = connRes.getLogLevel();
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

    try {
      this.tunnelReadTimeout = Integer.parseInt(connRes.getTunnelReadTimeout());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("tunnel-read-timeout is expected to be an integer");
    }

    try {
      this.tunnelConnectTimeout = Integer.parseInt(connRes.getTunnelConnectTimeout());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("tunnel-connect-timeout is expected to be an integer");
    }

    if (logLevel != null) {
      logLevel = logLevel.toUpperCase();

      switch (logLevel) {
        case "DEBUG":
          logLevel = "FINE";
          break;
        case "WARN":
          logLevel = "WARNING";
          break;
        case "ERROR":
          logLevel = "SEVERE";
          break;
        default:
          logLevel = "INFO";
      }
      this.logLevel = Level.parse(logLevel);
    }

    log = new OdpsLogger(this.getClass().getName(),
                         connectionId,
                         null,
                         logConfFile,
                         false,
                         connRes.isEnableOdpsLogger(),
                         this.logLevel);

    String version = Utils.retrieveVersion("driver.version");
    log.info("ODPS JDBC driver, Version " + version);
    log.info(String.format("endpoint=%s, project=%s, schema=%s", endpoint, project, schema));
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

    int retryTime = connRes.getRetryTime();
    if (retryTime > 0) {
      this.retryTime = retryTime;
      odps.getRestClient().setRetryTimes(this.retryTime);
    }

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
    this.enableCommandApi = connRes.isEnableCommandApi();
    this.httpsCheck = connRes.isHttpsCheck();
    this.skipSqlCheck = connRes.isSkipSqlRewrite();
    this.skipSqlInjectCheck = connRes.isSkipSqlInjectCheck();
    this.tunnelDownloadUseSingleReader = connRes.isTunnelDownloadUseSingleReader();
    this.useInstanceTunnel = connRes.isUseInstanceTunnel();
    this.verbose = connRes.isVerbose();
    this.async = connRes.isAsync();
    this.fetchResultSplitSize = connRes.getFetchResultSplitSize();
    this.fetchResultThreadNum = connRes.getFetchResultThreadNum();
    this.fetchResultPreloadSplitNum = connRes.getFetchResultPreloadSplitNum();

    if (!httpsCheck) {
      odps.getRestClient().setIgnoreCerts(true);
    }

    if (null == connRes.isOdpsNamespaceSchema()) {
      try {
        Tenant tenant = odps.tenant();
        this.odpsNamespaceSchema =
            Boolean.parseBoolean(tenant.getProperty(OdpsConstants.ODPS_NAMESPACE_SCHEMA));
      } catch (ReloadException e) {
        log.info("tenant doesn't exist, this project cannot support odpsNamespaceSchema.");
        this.odpsNamespaceSchema = false;
      }
    } else {
      this.odpsNamespaceSchema = connRes.isOdpsNamespaceSchema();
    }
    log.info("Support odps namespace schema: " + this.odpsNamespaceSchema);
    if (this.odpsNamespaceSchema) {
      sqlTaskProperties.put("odps.namespace.schema", "true");
    }
    this.catalogSchema = new CatalogSchema(odps, this.odpsNamespaceSchema);

    if (connRes.getQuotaName() != null) {
      try {
        enableMcqaV2 =
            odps.quotas().getWlmQuota(odps.getDefaultProject(), connRes.getQuotaName())
                .isInteractiveQuota();
        log.info("quotaName: " + connRes.getQuotaName() + ", enableMcqaV2: " + enableMcqaV2);
      } catch (Exception e) {
        try {
          log.warn(
              "check quotaName: " + connRes.getQuotaName() + " failed, enableMcqaV2: "
              + enableMcqaV2
              + " because " + e.getMessage());
          String tenantId = odps.projects().get().getTenantId();
          log.info("use project tenantId: " + tenantId);
        } catch (OdpsException ignored){}
      }
      quotaName = connRes.getQuotaName();
    }

    try {
      long startTime = System.currentTimeMillis();

      // Default value for odps.sql.timezone
      if (!StringUtils.isNullOrEmpty(connRes.getTimeZone())) {
        log.info("Use timezone: " + connRes.getTimeZone());
        sqlTaskProperties.put("odps.sql.timezone", connRes.getTimeZone());
        tz = TimeZone.getTimeZone(connRes.getTimeZone());
      } else {
        String projectTimeZoneId = odps.projects().get().getProperty("odps.sql.timezone");
        if (connRes.isUseProjectTimeZone() && !StringUtils.isNullOrEmpty(projectTimeZoneId)) {
          tz = TimeZone.getTimeZone(projectTimeZoneId);
        } else {
          tz = TimeUtils.UTC;
        }
      }
      log.info("Current connection timezone: " + tz.getID());

      long cost = System.currentTimeMillis() - startTime;
      log.info(String.format("load project meta infos time cost=%d", cost));
      initSQLExecutor(serviceName, fallbackPolicy);
      String msg = "Connect to odps project %s successfully";
      log.info(String.format(msg, odps.getDefaultProject()));

    } catch (OdpsException e) {
      log.error("Connect to odps failed:" + e.getMessage());
      throw new SQLException(e.getMessage(), e);
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
        .executeMode(interactiveMode ? ExecuteMode.INTERACTIVE : ExecuteMode.OFFLINE)
        .properties(hints)
        .serviceName(serviceName)
        .fallbackPolicy(fallbackPolicy)
        .enableReattach(true)
        .attachTimeout(attachTimeout)
        .quotaName(fallbackQuota)
        .tunnelEndpoint(tunnelEndpoint)
        .tunnelGetResultMaxRetryTime(tunnelRetryTime)
        .taskName(OdpsStatement.getDefaultTaskName())
        .enableCommandApi(enableCommandApi)
        .tunnelSocketTimeout(tunnelConnectTimeout)
        .tunnelReadTimeout(tunnelReadTimeout)
        .enableOdpsNamespaceSchema(odpsNamespaceSchema)
        .useInstanceTunnel(useInstanceTunnel)
        .logviewVersion(logviewVersion);

    if (enableMcqaV2) {
      builder.quotaName(quotaName);
      builder.enableMcqaV2(true);
    }
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
    OdpsPreparedStatement stmt;
    if (async) {
      stmt = new OdpsAsyncPreparedStatement(this, sql);
    } else {
      stmt = new OdpsPreparedStatement(this, sql);
    }
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
    OdpsPreparedStatement stmt;
    if (async) {
      stmt = new OdpsAsyncPreparedStatement(this, sql, isResultSetScrollable);
    } else {
      stmt = new OdpsPreparedStatement(this, sql, isResultSetScrollable);
    }
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
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
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
    OdpsStatement stmt;
    if (async) {
      stmt = new OdpsAsyncStatement(this, false);
    } else {
      stmt = new OdpsStatement(this, false);
    }
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
    OdpsStatement stmt;
    if (async) {
      stmt = new OdpsAsyncStatement(this, isResultSetScrollable);
    } else {
      stmt = new OdpsStatement(this, isResultSetScrollable);
    }
    stmtHandles.add(stmt);
    return stmt;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    log.warn("Ingore resultSetHoldability when create Statement");
    return createStatement(resultSetType, resultSetConcurrency);
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

  public TimeZone getTimezone() {
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

  public void setTunnelEndpoint(String tunnelEndpoint) throws OdpsException {
    this.tunnelEndpoint = tunnelEndpoint;
    initSQLExecutor(serviceName, fallbackPolicy);
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

  public int getRetryTime() {
    if (retryTime <= 0) {
      return odps.getRestClient().getRetryTimes();
    }
    return retryTime;
  }

  public void setRetryTime(int retryTime) {
    if (retryTime <= 0) {
      throw new IllegalArgumentException("retry-times should be positive.");
    }
    this.retryTime = retryTime;
    odps.getRestClient().setRetryTimes(this.retryTime);
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

  public int getTunnelReadTimeout() {
    return tunnelReadTimeout;
  }

  public int getTunnelConnectTimeout() {
    return tunnelConnectTimeout;
  }

  public boolean isEnableCommandApi() {
    return enableCommandApi;
  }

  public boolean isHttpsCheck() {
    return httpsCheck;
  }

  public boolean isSkipSqlCheck() {
    return skipSqlCheck;
  }

  public boolean isSkipSqlInjectCheck() {
    return skipSqlInjectCheck;
  }

  public boolean isTunnelDownloadUseSingleReader() {
    return tunnelDownloadUseSingleReader;
  }

  public boolean isUseInstanceTunnel() {
    return useInstanceTunnel;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public long getFetchResultSplitSize() {
    return fetchResultSplitSize;
  }

  public int getFetchResultThreadNum() {
    return fetchResultThreadNum;
  }

  public int getFetchResultPreloadSplitNum() {
    return fetchResultPreloadSplitNum;
  }

  public void setUseInstanceTunnel(boolean useInstanceTunnel) throws OdpsException {
    this.useInstanceTunnel = useInstanceTunnel;
    initSQLExecutor(serviceName, fallbackPolicy);
  }

  public void setInteractiveMode(boolean interactiveMode) throws OdpsException {
    this.interactiveMode = interactiveMode;
    initSQLExecutor(serviceName, fallbackPolicy);
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