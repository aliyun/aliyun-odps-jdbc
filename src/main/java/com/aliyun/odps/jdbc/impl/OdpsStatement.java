package com.aliyun.odps.jdbc.impl;

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.csvreader.CsvReader;

public class OdpsStatement extends WrapperAdapter implements Statement {

    private TaskSummary    taskSummary;
    private int            updateCount  = -1;

    private OdpsConnection conn;
    private Instance       instance;
    private String         result;

    private int            resultSetHoldability;
    private int            resultSetConcurrency;
    private int            resultSetType;

    private boolean        closeOnCompletion;
    private boolean        closed       = false;
    private int            maxFieldSize;
    private int            maxRows;
    private int            fetchSize;
    private int            queryTimeout = -1;
    private int            fetchDirection;
    private boolean        poolable     = false;

    OdpsStatement(OdpsConnection conn){
        this.conn = conn;
    }

    OdpsStatement(OdpsConnection conn, int resultSetType, int resultSetConcurrency){
        this.conn = conn;
    }

    OdpsStatement(OdpsConnection conn, int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        this.conn = conn;
    }

    Instance getOdpsTaskIntance() {
        return this.instance;
    }

    String readRunSqlResult() throws SQLException {
        try {
            Map<String, String> results = instance.getTaskResults();
            Map<String, TaskStatus> taskStatus = instance.getTaskStatus();

            for (Entry<String, TaskStatus> status : taskStatus.entrySet()) {
                result = results.get(status.getKey());
            }
        } catch (OdpsException e) {
            throw new SQLException(e.getMessage(), e);
        }
        return result;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        boolean firstResultSet = execute(sql);
        if (!firstResultSet) {
            return null;
        }

        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        execute(sql);

        return updateCount;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

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
    public void cancel() throws SQLException {
        if (instance == null) {
            return;
        }

        try {
            instance.stop();
        } catch (OdpsException e) {
            throw new SQLException("cancel error", e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        beforeExecute();

        instance = conn.run(sql);

        try {
            taskSummary = instance.getTaskSummary("SQL");
        } catch (Exception e) {
            throw new SQLException("get task summary error", e);
        }

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

        result = readRunSqlResult();

        if (result != null && !result.isEmpty()) {
            return true;
        }

        return false;
    }

   

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (result == null || result.isEmpty()) {
            return null;
        }

        try {
            CsvReader resultReader = new CsvReader(new StringReader(result), ',');
            resultReader.setSafetySwitch(false);

            String[] columns;
            if (resultReader.readRecord()) {
                columns = resultReader.getValues();
            } else {
                throw new SQLException();
            }

            int[] types = new int[columns.length];
            Arrays.fill(types, Types.VARCHAR);

            OdpsResultSetMetaData meta = new OdpsResultSetMetaData(columns, types);

            return new OdpsQueryResultSet(this, meta, resultReader);
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {

        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {

        return null;
    }

    @Override
    public OdpsConnection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {

        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {

        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        execute(sql, autoGeneratedKeys);
        return getUpdateCount();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        execute(sql, columnIndexes);
        return getUpdateCount();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        execute(sql, columnNames);
        return getUpdateCount();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    public boolean isPoolable() throws SQLException {
        return poolable;
    }

    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    void beforeExecute() {
        instance = null;
        updateCount = -1;
        result = null;
        taskSummary = null;
    }
}
