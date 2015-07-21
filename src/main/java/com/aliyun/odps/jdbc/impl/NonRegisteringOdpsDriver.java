package com.aliyun.odps.jdbc.impl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;

public class NonRegisteringOdpsDriver implements Driver {

  public final static String URL_PREFIX = "jdbc:odps:";

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
