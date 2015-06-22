package com.aliyun.odps.jdbc.impl;

import java.lang.reflect.Field;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.SQLTask;

public class OdpsConnection extends WrapperAdapter implements Connection {

    private Odps                 odps;

    private boolean              closed     = false;

    private OdpsDatabaseMetaData meta;

    private String               schema;
    private String               catalog;

    private String               url;

    private int                  taskIdSeed = 1000000;

    OdpsConnection(Odps odps, String url){
        this.odps = odps;
        this.url = url;
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
    public PreparedStatement prepareStatement(String sql) throws SQLException {

        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {

        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {

        return null;
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

        return 0;
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

        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        return null;
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

        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {

        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new OdpsStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                           throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {

        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {

        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {

        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {

        return null;
    }

    @Override
    public Clob createClob() throws SQLException {

        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {

        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {

        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {

        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {

        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {

        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {

        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {

        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {

        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        if (this.schema != null) {
            return this.schema;
        }

        return odps.getDefaultProject();
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    protected Instance run(String sql, String taskName) throws SQLException {
        Instance instance;
        try {
            Map<String, String> hints = new HashMap<String, String>();
            Map<String, String> aliases = new HashMap<String, String>();
            run(odps, odps.getDefaultProject(), sql, taskName, hints, aliases, null, "sql");
            instance = SQLTask.run(odps, sql);
        } catch (OdpsException e) {
            throw new SQLException("run sql error", e);
        }

        boolean terminated = false;
        while (!terminated) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new SQLException("run sql error", e);
            }
            terminated = instance.isTerminated();
        }

        return instance;
    }

    Instance run(Odps odps, String project, String sql, String taskName, Map<String, String> hints,
                 Map<String, String> aliases, Integer priority, String type) throws OdpsException {
        String guid = UUID.randomUUID().toString();

        SQLTask task = new SQLTask();
        task.setQuery(sql);
        task.setName(taskName);
        task.setProperty("type", type);
        task.setProperty("guid", guid);
        
        if (hints != null) {
            try {
                String json = JacksonParser.getObjectMapper().writeValueAsString(hints);
                task.setProperty("settings", json);
            } catch (Exception e) {
                throw new OdpsException(e.getMessage(), e);
            }

        }

        if (aliases != null) {
            try {
                String json = JacksonParser.getObjectMapper().writeValueAsString(aliases);
                task.setProperty("aliases", json);
            } catch (Exception e) {
                throw new OdpsException(e.getMessage(), e);
            }

        }

        if (priority != null) {
            return odps.instances().create(project, task, priority);
        } else {
            return odps.instances().create(project, task);
        }
    }
    


    public TaskSummary getTaskSummaryV1(Instance instance, String taskName) throws Exception {
        RestClient client = odps.getRestClient();
        Map<String, String> params = new HashMap<String, String>();
        params.put("summary", null);
        params.put("taskname", taskName);
        String queryString = "/projects/" + instance.getProject() + "/instances/" + instance.getId();
        Response result = client.request(queryString, "GET", params, null, null);

        TaskSummary summary = null;
        ObjectMapper objMapper = JacksonParser.getObjectMapper();
        Map<Object, Object> map = objMapper.readValue(result.getBody(), Map.class);
        if (map.get("mapReduce") != null) {
            Map mapReduce = (Map) map.get("mapReduce");
            String jsonSummary = (String) mapReduce.get("jsonSummary");
            summary = new TaskSummary();
            if (jsonSummary == null) {
                jsonSummary = "{}";
            }
            if (summary != null) {
                Field textFiled = summary.getClass().getDeclaredField("text");
                textFiled.setAccessible(true);
                textFiled.set(summary, (String) mapReduce.get("summary"));
                Field jsonField = summary.getClass().getDeclaredField("jsonSummary");
                jsonField.setAccessible(true);
                jsonField.set(summary, jsonSummary);
            }
        }
        return summary;
    }
}
