package com.aliyun.openservices.odps.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class OdpsJDBCDriver implements Driver {
    static {
        try {
            java.sql.DriverManager.registerDriver(new OdpsJDBCDriver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    /*
     * url:include(projectName) info:include(userName,password)
     * 
     * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        assert (acceptsURL(url));
        if (!acceptsURL(url)) {
            throw new SQLException(OdpsJDBCConstants.URL_ERROR);
        }
        OdpsJDBCConnection con = new OdpsJDBCConnection();
        con.init(url, info);
        return con;
    }
    /*
     * validate the Url
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return validateUrl(url);
    }

    private boolean validateUrl(String url) {
        if (url.matches(".*/projects/.*"))
            return true;
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        throw new SQLException(OdpsJDBCConstants.METHOD_NOT_SUPPORT);
    }

    @Override
    public int getMajorVersion() {
        return OdpsJDBCConstants.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return OdpsJDBCConstants.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

}
