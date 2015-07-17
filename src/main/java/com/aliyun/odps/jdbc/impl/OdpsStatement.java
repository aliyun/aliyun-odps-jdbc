package com.aliyun.odps.jdbc.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.RecordReader;

public class OdpsStatement extends WrapperAdapter implements Statement {
    private TaskSummary taskSummary;
    private int updateCount = -1;

    private OdpsConnection conn;
    private Instance instance;

    private ResultSet resultSet = null;

    private int resultSetHoldability;
    private int resultSetConcurrency;
    private int resultSetType;

    private int maxFieldSize;
    private int maxRows;
    private int fetchSize;
    private int queryTimeout = -1;
    private int fetchDirection;
    private boolean isClosed = false;
    private boolean isCancelled = false;

    OdpsStatement(OdpsConnection conn) {
        this.conn = conn;
    }

    OdpsStatement(OdpsConnection conn, int resultSetType, int resultSetConcurrency) {
        this.conn = conn;
    }

    OdpsStatement(OdpsConnection conn, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
        this.conn = conn;
    }

    @Override public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void cancel() throws SQLException {
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

    @Override public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void close() throws SQLException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        resultSet = null;
    }

    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public ResultSet executeQuery(String sql) throws SQLException {
        beforeExecute();

        // Generate the resultSet by creating a temperate table
        String uuid = UUID.randomUUID().toString().replaceAll("-", "_");
        String tempTableName = "tmp_table_for_select_jdbc_" + uuid;

        // TODO
        String subsql = sql.substring(0, sql.length() - 1);
        String createTempTableSql =
            "create table " + tempTableName + " as select * from (" + subsql + ")" + tempTableName
                + " limit 10000;";
        String dropTempTableSql = "drop table " + tempTableName + ";";

        instance = conn.run(createTempTableSql);

        // Read the table schema
        Table table = conn.getOdps().tables().get(tempTableName);
        List<String> columnNames = new ArrayList<String>();
        List<OdpsType> columnSqlTypes = new ArrayList<OdpsType>();
        try {
            table.reload();
            for (Column col : table.getSchema().getColumns()) {
                columnNames.add(col.getName());
                columnSqlTypes.add(col.getType());
            }
        } catch (OdpsException e) {
            throw new SQLException("cannot read table schema", e);
        }
        OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columnNames, columnSqlTypes);

        // Read the table records
        RecordReader recordReader;
        try {
            recordReader = table.read(10000);
        } catch (OdpsException e) {
            throw new SQLException("can not read table records", e);
        }

        resultSet = new OdpsQueryResultSet(this, meta, recordReader);

        // TODO: when?
        instance = conn.run(dropTempTableSql);
        return resultSet;
    }

    @Override public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override public int executeUpdate(String sql) throws SQLException {
        beforeExecute();

        instance = conn.run(sql);

        try {
            taskSummary = instance.getTaskSummary("SQL");
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

    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        execute(sql, autoGeneratedKeys);
        return getUpdateCount();
    }

    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public OdpsConnection getConnection() throws SQLException {
        return conn;
    }

    @Override public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    @Override public void setMaxFieldSize(int max) throws SQLException {
        maxFieldSize = max;
    }

    @Override public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max must be >= 0");
        }
        this.maxRows = max;
    }

    @Override public boolean getMoreResults() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    //    Instance getOdpsTaskIntance() {
    //        return this.instance;
    //    }

    @Override public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override public boolean isClosed() throws SQLException {
        return isClosed;
    }

    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void beforeExecute() {
        instance = null;
        updateCount = -1;
        resultSet = null;
        taskSummary = null;
        isCancelled = false;
    }
}
