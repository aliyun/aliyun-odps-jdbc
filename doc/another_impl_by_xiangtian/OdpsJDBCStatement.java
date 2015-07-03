package com.aliyun.openservices.odps.jdbc;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.aliyun.common.comm.ResponseMessage;
import com.aliyun.openservices.ClientException;
import com.aliyun.openservices.HttpMethod;
import com.aliyun.openservices.odps.ODPSException;
import com.aliyun.openservices.odps.internal.ODPSConstants;
import com.aliyun.openservices.odps.jobs.Job;
import com.aliyun.openservices.odps.jobs.JobInstance;
import com.aliyun.openservices.odps.jobs.SqlTask;
import com.aliyun.openservices.odps.tables.RecordReader;
import com.aliyun.openservices.odps.tables.Table;

public class OdpsJDBCStatement implements Statement {
    private OdpsJDBCConnection connection;

    public void init(OdpsJDBCConnection odpsJDBCConn) {
        this.connection = odpsJDBCConn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (sql.trim().toUpperCase().matches("SELECT.*")) {
            OdpsJDBCQueryData queryData = runSelect(sql);
            OdpsJDBCResultSet rs = new OdpsJDBCResultSet();
            try {
                rs.init(this, queryData);
            } catch (ClientException e) {
                e.printStackTrace();
            }
            return rs;
        } else {
            runsql(sql);
            OdpsJDBCResultSet rs = new OdpsJDBCResultSet();
            rs.init(this);
            return rs;
        }
    }
        
    private OdpsJDBCQueryData runSelect(String sql) {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "_");
        String uuidTableName = "tmp_table_for_select_jdbc_"
                + UUID.randomUUID().toString().replaceAll("-", "_");
        String createAsString = "create table " + uuidTableName
                + " as select * from (" + sql + ")" + uuid + " limit 10000";
        runsql(createAsString);
        Table table = new Table(connection.getProject(), uuidTableName);
        RecordReader reader = null;
        Map<String, String> fieldTypeMap=null;
        try {
            table.load();
            fieldTypeMap=tableMeta(table.getSchema().toJson());
            reader = table.readData(null, null, -1);
        } catch (ODPSException e1) {
            e1.printStackTrace();
        } catch (ClientException e1) {
            e1.printStackTrace();
        }
        String dropString = "drop table " + uuidTableName;
        runsql(dropString);
        return new OdpsJDBCQueryData(reader,fieldTypeMap);
    }

    private Map<String, String> tableMeta(String json){
        try {
            JSONObject jSONObject = new JSONObject(json);
            Map<String, String> fieldTypeMap = new LinkedHashMap<String, String>();
            // Get Column Info from Table Meta
            JSONArray columnArray = (JSONArray) jSONObject.get("columns");
            for (int i = 0; i < columnArray.length(); i++) {
                JSONObject column = (JSONObject) columnArray.get(i);
                fieldTypeMap.put(column.getString("name"),
                        column.getString("type"));
            }
            // Get Partition Info from Table Meta
            JSONArray partitionArray = (JSONArray) jSONObject
                    .get("partitionKeys");
            for (int i = 0; i < partitionArray.length(); i++) {
                JSONObject partition = (JSONObject) partitionArray.get(i);
                fieldTypeMap.put(partition.getString("name"),
                        partition.getString("type"));
            }     
            return fieldTypeMap;
        } catch (JSONException e) {
        }
        return null;
        
    }
    

    private String runsql(String sql) {
        String taskName = "jdbc_query_task_"
                + Calendar.getInstance().getTimeInMillis();
        sql+=";";
        SqlTask task = new SqlTask(taskName, sql);
        // TaskConfig taskConfig = getTaskConfig();
        // task.setConfig(taskConfig);
        try {
            JobInstance instance = Job.run(connection.getProject(), task);
            while (!instance.getStatus().equals(JobInstance.Status.TERMINATED)) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (ODPSException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }
    
 // ReadData from API,Get InputStream from API
    private InputStream readData(String partition, List<String> columns,
            int top, String tableName) throws ODPSException, ClientException {
        Map<String, String> parameters = new LinkedHashMap<String, String>();
        parameters.put("data", null);
        if (partition != null && partition.length() != 0) {
            parameters.put("partition", partition);
        }
        if (columns != null && columns.size() != 0) {
            String column = "";
            for (String temp : columns) {
                column += temp;
                column += ",";
            }
            column = column.substring(0, column.lastIndexOf(","));
            parameters.put("cols", column);
        }
        if (top != -1) {
            parameters.put("linenum", String.valueOf(top));
        }
        ResponseMessage response = connection
                .getProject()
                .getConnection()
                .request(getUriPath(tableName), HttpMethod.GET, null,
                        parameters, null, 0);
        return response.getContent();

    }

    //
    private String getUriPath(String tableName) {
        return connection.getProject().getUriPath()+ODPSConstants.ODPS_TABLES_PATH+tableName;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getMaxRows() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

}
