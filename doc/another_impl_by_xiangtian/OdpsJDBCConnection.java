package com.aliyun.openservices.odps.jdbc;

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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.aliyun.openservices.odps.ODPSConnection;
import com.aliyun.openservices.odps.Project;

public class OdpsJDBCConnection implements Connection {
    private Project project;

    // create the ODPSConnection
    /*
     *  Replace with Constructor
     */
    public void init(String url, Properties info) throws SQLException {
        ODPSConnection odpsConnection = new ODPSConnection(
                getConnctionUrl(url), info.getProperty("user"),
                info.getProperty("password"));
        project = new Project(odpsConnection, getProjectName(url));
    }

    /*
     * extract url from the url
     */
    private String getConnctionUrl(String url) throws SQLException {
        if (url != null && url.indexOf("/projects/") > 1) {
            return url.substring(0, url.indexOf("/projects/"));
        }
        throw new SQLException("Invalid URL:" +url);
    }

    /*
     * extract projectName from url
     */
    private String getProjectName(String url) throws SQLException {
        if (url != null && url.indexOf("/projects/") > 1) {
            return url.substring(url.indexOf("/projects/") + 10);
        }
        throw new SQLException("Can't extract projectName from Url:" +url);
    }

    public Project getProject() {
        return project;
    }
   
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    
    @Override
    public Statement createStatement() throws SQLException {
        // TODO Auto-generated method stub
        OdpsJDBCStatement stmt = new OdpsJDBCStatement();
        stmt.init(this);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public void commit() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void rollback() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public String getCatalog() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

}
