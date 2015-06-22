package com.aliyun.odps.jdbc.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.cli.commands.StatusCommand;
import com.csvreader.CsvReader;

public class OdpsStatement extends WrapperAdapter implements Statement {

    private OdpsConnection conn;
    private Instance       instance;
    private String         result;

    OdpsStatement(OdpsConnection conn){
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
        String taskName = conn.generateTaskName();
        instance = conn.run(sql, taskName);

        String result = readRunSqlResult();
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
    public int executeUpdate(String sql) throws SQLException {
        String taskName = conn.generateTaskName();
        instance = conn.run(sql, taskName);

        String result = readRunSqlResult();

        PrintWriter stdout = new PrintWriter(System.out);
       
        try {
            StatusCommand.printResults(instance, stdout);
            StatusCommand.printSummary(instance, stdout);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public int getMaxFieldSize() throws SQLException {

        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {

        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {

        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

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
        String taskName = conn.generateTaskName();
        Instance instance = conn.run(sql, taskName);
        TaskSummary summary = null;
        try {
            summary = conn.getTaskSummaryV1(instance, taskName);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {

        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {

        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {

        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {

        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {

        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {

        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {

        return 0;
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

        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {

        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {

        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {

        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {

        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {

        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {

        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {

        return false;
    }

    public void setPoolable(boolean poolable) throws SQLException {

    }

    public boolean isPoolable() throws SQLException {

        return false;
    }

    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {

        return false;
    }

    TaskStatus getTaskStatus(final Task task) throws OdpsException {

        TaskStatus taskStatus = instance.getTaskStatus().get(task.getName());
        if (taskStatus == null) {
            // 如果task还没有进行，被kill掉了会出异常
            throw new OdpsException("can not get task status.");
        }

        return taskStatus;
    }
}
