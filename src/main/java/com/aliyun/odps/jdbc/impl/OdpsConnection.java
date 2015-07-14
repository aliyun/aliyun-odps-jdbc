package com.aliyun.odps.jdbc.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

public class OdpsConnection extends WrapperAdapter implements Connection {

  private PrintWriter stdout = new PrintWriter(System.out);

  private Odps odps;

  private boolean closed = false;

  private OdpsDatabaseMetaData meta;

  private String schema;
  private String catalog;
  private String url;

  private int taskIdSeed = 1000000;

  private Properties info;

  OdpsConnection(String url, Properties info) {
    String accessId = info.getProperty("access_id");
    String accessKey = info.getProperty("access_key");
    String project = info.getProperty("project_name");

    Account account = new AliyunAccount(accessId, accessKey);
    this.odps = new Odps(account);
    this.url = url;
    this.info = info;

    odps.setDefaultProject(project);
    odps.setEndpoint(url);
  }

  public Odps getOdps() {
    return this.odps;
  }

  String getUrl() {
    return this.url;
  }

  String generateTaskName() {
    return "jdbc_" + (taskIdSeed++);
  }

  @Override
  public OdpsPreparedStatement prepareStatement(String sql) throws SQLException {
    return new OdpsPreparedStatement(this, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return new OdpsCallableStatement(this, sql);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      return;
    }

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException {
    closed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (meta == null) {
      meta = new OdpsDatabaseMetaData(this);
    }

    return meta;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    this.odps.setDefaultProject(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    if (catalog != null) {
      return catalog;
    }

    return odps.getDefaultProject();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {

  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {

    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {

    return new OdpsPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {

    return new OdpsCallableStatement(this, sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {

    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

  @Override
  public void setHoldability(int holdability) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {

    return 0;
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
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public OdpsStatement createStatement() throws SQLException {
    return new OdpsStatement(this);
  }

  @Override
  public OdpsStatement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return new OdpsStatement(this, resultSetType, resultSetConcurrency);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return new OdpsStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {

    return new OdpsPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {

    return new OdpsCallableStatement(this, sql, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

    return new OdpsPreparedStatement(this, sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {

    return new OdpsPreparedStatement(this, sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {

    return new OdpsPreparedStatement(this, sql, columnNames);
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
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    this.info.put(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    this.info.putAll(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return info.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return info;
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchema() throws SQLException {
    if (this.schema != null) {
      return this.schema;
    }

    return odps.getDefaultProject();
  }

  public void abort(Executor executor) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public int getNetworkTimeout() throws SQLException {
    return odps.getRestClient().getConnectTimeout();
  }

  protected Instance run(String sql) throws SQLException {
    Instance instance;
    try {
      Map<String, String> hints = new HashMap<String, String>();
      Map<String, String> aliases = new HashMap<String, String>();

      instance = SQLTask.run(odps, odps.getDefaultProject(), sql, "SQL", hints, aliases);

      PrintWriter out = this.getStdout();
      out.println("ID = " + instance.getId());

      LogView logView = new LogView(odps);
      String logViewUrl = logView.generateLogView(instance, 7 * 24);
      out.println("Log View :");
      out.println(logViewUrl);

      waiting(instance);
    } catch (OdpsException e) {
      throw new SQLException("run sql error", e);
    }

    return instance;
  }

  public void waiting(Instance instance) throws OdpsException {
    boolean newLine = true;

    PrintWriter out = this.getStdout();

    boolean terminated = false;

    String blankLine = buildString(' ', FIXED_WIDTH);

    int round = 0;

    while (!terminated) {

      terminated = instance.isTerminated();

      if (terminated) {
        return;
      }

      if (!newLine) {
        out.print(blankLine);
        out.print('\r');
      }

      out.print(getStageProgress(instance, round));
      round++;

      if (!newLine) {
        out.print('\r');
      } else {
        out.println();
      }

      out.flush();

    }

    out.println();
  }

  private static final int FIXED_WIDTH = 100;

  public static String getStageProgress(Instance instance, int round) throws OdpsException {
    StringBuilder sb = new StringBuilder();

    Set<String> taskNames = instance.getTaskNames();

    StringWriter strWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(strWriter);
    int i = 0;
    for (String taskName : taskNames) {
      List<StageProgress> stages = instance.getTaskProgress(taskName);

      if (stages.size() == 0) {
        writer.print(taskName + ": " + buildString('.', round % 3 + 1));
      } else {
        writer.print(taskName + ":");
        for (StageProgress stage : stages) {
          writer.printf(" %s:%s/%s/%s", stage.getName(), stage.getRunningWorkers(),
              stage.getTerminatedWorkers(), stage.getTotalWorkers());
        }
      }
      if (++i < taskNames.size()) {
        writer.print(", ");
      }
    }

    String str = strWriter.toString();
    String padding =
        str.length() >= FIXED_WIDTH ? "" : buildString(' ', FIXED_WIDTH - str.length());
    sb.append(str);
    sb.append(padding);
    return sb.toString();
  }

  private static String buildString(char c, int n) {
    if (n < 0)
      throw new IllegalArgumentException();

    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(c);
    }
    return sb.toString();
  }

  public PrintWriter getStdout() {
    return stdout;
  }

  public void setStdout(PrintWriter stdout) {
    this.stdout = stdout;
  }


}
