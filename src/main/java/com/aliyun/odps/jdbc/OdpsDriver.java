package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class OdpsDriver implements Driver {
  static {
    try {
      DriverManager.registerDriver(new OdpsDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public final static String URL_PREFIX = "jdbc:odps:";

  public OdpsDriver() {
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkWrite("odps");
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    String tuncatedUrl = url.substring(URL_PREFIX.length());
    return new OdpsConnection(tuncatedUrl, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url != null && url.startsWith(URL_PREFIX)) {
      return true;
    }
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    return null;
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

}
