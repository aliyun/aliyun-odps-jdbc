package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.jdbc.utils.SettingParser;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsAsyncPreparedStatement extends OdpsPreparedStatement {

  OdpsAsyncPreparedStatement(OdpsConnection conn, String sql) {
    super(conn, sql);
  }

  OdpsAsyncPreparedStatement(OdpsConnection conn, String sql, boolean isResultSetScrollable) {
    super(conn, sql, isResultSetScrollable);
  }


  @Override
  public synchronized boolean execute(String query) throws SQLException {
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

  private void runSQL(String sql, Properties properties) throws SQLException {
    SQLExecutor executor = this.sqlExecutor;
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
      executor.run(sql, settings);
      connHandle.log.info("Run SQL: [" + sql + "],submit cost: " + (System.currentTimeMillis() - begin) + "ms");
      logviewUrl = executor.getLogView();
      connHandle.log.info("LogView: " + logviewUrl);
      executeInstance = executor.getInstance();
      if (executeInstance != null) {
        connHandle.log.info("InstanceId: " + executeInstance.getId());
      }
    } catch (OdpsException e) {
      throwSQLException(e, sql, executor.getInstance(), executor.getLogView());
    }
  }

  @Override
  public synchronized ResultSet getResultSet() throws SQLException {
    SQLExecutor executor = this.sqlExecutor;
    try {
      long startTime = System.currentTimeMillis();
      setResultSetInternal();
      connHandle.log.info("Get result set, cost time: " + (System.currentTimeMillis() - startTime) + "ms");
      List<String> exeLog = executor.getExecutionLog();
      if (!exeLog.isEmpty()) {
        for (String log : exeLog) {
          connHandle.log.info("Session execution log: " + log);
        }
      }
      return super.getResultSet();
    } catch (OdpsException | IOException e) {
      throwSQLException(e, "unknown", executor.getInstance(), executor.getLogView());
      return null;
    }
  }
}
