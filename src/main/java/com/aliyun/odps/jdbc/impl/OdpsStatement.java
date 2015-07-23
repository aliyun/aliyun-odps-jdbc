package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsStatement extends WrapperAdapter implements Statement {

  // The table name must be unique since there can be two statements querying its results.
  private static final String TEMP_TABLE_NAME =
      "temp_tbl_for_query_result_" + UUID.randomUUID().toString().replaceAll("-", "_");

  private OdpsConnection conn;
  private Instance instance = null;
  private ResultSet resultSet = null;
  private int updateCount = -1;
  private int maxRows;
  private int fetchSize;
  private int queryTimeout = -1;
  private boolean isClosed = false;
  private boolean isCancelled = false;

  OdpsStatement(OdpsConnection conn) {
    this.conn = conn;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancel() throws SQLException {
    if (isCancelled || instance == null) {
      return;
    }

    try {
      instance.stop();
    } catch (OdpsException e) {
      throw new SQLException("cancel error", e);
    }

    isCancelled = true;
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }


  /**
   * Each resultSet is associated with a statement. If the same statement to execute
   * another query, the original resultSet must be released. The temp table used to
   * generate the result must be dropped as well.
   *
   * @throws SQLException
   */
  @Override
  public void close() throws SQLException {
    if (isClosed || resultSet == null) {
      return;
    }

    // Drop the temp table for querying result set and ensure it has been dropped
    Instance dropTableInstance = conn.run(String.format("drop table %s;", TEMP_TABLE_NAME));
    Instance.TaskStatus dropTableStatus;
    try {
      Map<String, Instance.TaskStatus> statuses = dropTableInstance.getTaskStatus();
      dropTableStatus = statuses.get("SQL");
    } catch (OdpsException e) {
      throw new SQLException("can not read instance status", e);
    }
    if (dropTableStatus.getStatus() != Instance.TaskStatus.Status.SUCCESS) {
      throw new SQLException("can not drop the temp table for querying result");
    }

    isClosed = true;
    resultSet = null;
  }

  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {

    beforeExecute();

    // Drop the temp table for querying result set
    // in case that the table has been created in the former execution.
    Instance dropTableInstance =
        conn.run(String.format("drop table if exists %s;", TEMP_TABLE_NAME));
    Instance.TaskStatus dropTableStatus;
    try {
      Map<String, Instance.TaskStatus> statuses = dropTableInstance.getTaskStatus();
      dropTableStatus = statuses.get("SQL");
    } catch (OdpsException e) {
      throw new SQLException("can not read instance status", e);
    }
    if (dropTableStatus.getStatus() != Instance.TaskStatus.Status.SUCCESS) {
      throw new SQLException("can not drop the temp table for querying result");
    }

    // Create a temp table for querying result set and ensure its creation
    Instance createTableInstance =
        conn.run(String.format("create table %s as %s", TEMP_TABLE_NAME, sql));

    Instance.TaskStatus createTableStatus;
    try {
      Map<String, Instance.TaskStatus> statuses = createTableInstance.getTaskStatus();
      createTableStatus = statuses.get("SQL");
    } catch (OdpsException e) {
      throw new SQLException("can not read instance status", e);
    }
    if (createTableStatus.getStatus() != Instance.TaskStatus.Status.SUCCESS) {
      throw new SQLException("can not create temp table for querying result");
    }

    // Read schema
    Table table = conn.getOdps().tables().get(TEMP_TABLE_NAME);
    List<String> columnNames = new ArrayList<String>();
    List<OdpsType> columnSqlTypes = new ArrayList<OdpsType>();
    try {
      table.reload();
      for (Column col : table.getSchema().getColumns()) {
        columnNames.add(col.getName());
        columnSqlTypes.add(col.getType());
      }
    } catch (OdpsException e) {
      throw new SQLException("can not read table schema", e);
    }
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);

    // Read records
    RecordReader recordReader;
//    try {
//      recordReader = table.read(10000);
//    } catch (OdpsException e) {
//      throw new SQLException("can not read table records", e);
//    }

    // Read record through tunnel
    TableTunnel tunnel = new TableTunnel(conn.getOdps());
    try {
      DownloadSession downloadSession =
          tunnel.createDownloadSession(conn.getOdps().getDefaultProject(), TEMP_TABLE_NAME);
      long count = downloadSession.getRecordCount();
      recordReader = downloadSession.openRecordReader(0, count);
    } catch (TunnelException e) {
      throw new SQLException("can not create tunnel download session", e);
    } catch (java.io.IOException e) {
      throw new SQLException("can not open record reader in download session", e);
    }


    resultSet = new OdpsQueryResultSet(this, meta, recordReader);
    return resultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    beforeExecute();

    instance = conn.run(sql);

    try {
      Instance.TaskSummary taskSummary = instance.getTaskSummary("SQL");
      if (taskSummary != null) {
        JSONObject jsonSummary = JSON.parseObject(taskSummary.getJsonSummary());
        JSONObject outputs = jsonSummary.getJSONObject("Outputs");

        if (outputs.size() > 0) {
          updateCount = 0;
          for (Object item : outputs.values()) {
            JSONArray array = (JSONArray) item;
            updateCount += array.getInteger(0);
          }
        }
      }
    } catch (OdpsException e) {
      throw new SQLException("get task summary error", e);
    }

    return updateCount;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    execute(sql, autoGeneratedKeys);
    return getUpdateCount();
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
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
  public boolean execute(String sql) throws SQLException {
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
    return conn;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
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
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max must be >= 0");
    }
    this.maxRows = max;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    this.queryTimeout = seconds;
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
  public int getUpdateCount() throws SQLException {
    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
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
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLException("Not supported direction: " + direction);
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  private void beforeExecute() {
    instance = null;
    updateCount = -1;
    resultSet = null;
    isCancelled = false;
  }
}
