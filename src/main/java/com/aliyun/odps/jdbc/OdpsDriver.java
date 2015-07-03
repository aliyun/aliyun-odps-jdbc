package com.aliyun.odps.jdbc;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.aliyun.odps.jdbc.impl.NonRegisteringOdpsDriver;

public class OdpsDriver extends NonRegisteringOdpsDriver implements Driver {

  public final static OdpsDriver instance = new OdpsDriver();

  static {
    AccessController.doPrivileged(new PrivilegedAction<Object>() {

      @Override
      public Object run() {
        registerDriver(instance);
        return null;
      }
    });
  }

  public static boolean registerDriver(Driver driver) {
    try {
      DriverManager.registerDriver(driver);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return true;
  }

}
