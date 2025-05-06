package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InstanceTunnelTest {
  private static Connection conn;

  public static void prepare() {
    try {
      String driverName = "com.aliyun.odps.jdbc.OdpsDriver";
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // fill in the information string
    Properties odpsConfig = new Properties();
    InputStream is =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
    try {
      odpsConfig.load(is);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    String accessId = odpsConfig.getProperty("access_id");
    String accessKey = odpsConfig.getProperty("access_key");
    String endpoint = odpsConfig.getProperty("end_point");
    String pj = odpsConfig.getProperty("project_name");
    conn = null;
    try {
      conn =
          DriverManager.getConnection(
              "jdbc:odps:" + endpoint + "?project=" + pj + "&enableOdpsLogger=true&enableLimit=false", accessId,
              accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowCreateTable() throws Exception {
    prepare();
    Statement statement = conn.createStatement();
    ResultSet resultSet = statement.executeQuery("show create table aaa;");
    boolean next = resultSet.next();
    while (next) {
      System.out.println(resultSet.getString(1));
      next = resultSet.next();
    }
  }



}
