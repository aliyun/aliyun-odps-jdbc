package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class JdbcTransformerTest {

  private static Connection conn;

  @BeforeClass
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
              "jdbc:odps:" + endpoint + "?project=" + pj
              + "&enableOdpsLogger=true&useProjectTimeZone=true", accessId,
              accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDate() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));

    Statement statement = conn.createStatement();
    ResultSet
        resultSet =
        statement.executeQuery(
            "set odps.sql.timezone=UTC;" +
            "select DATE'2024-09-30', DATETIME'2024-09-30 00:00:00', TIMESTAMP'2024-09-30 00:00:00.123', TIMESTAMP_NTZ'2024-09-30 00:00:00.123';");

    resultSet.next();
    for (int i = 1; i <= 4; i++) {
      Assert.assertEquals(1727654400000L, resultSet.getDate(i).getTime());
    }

    resultSet =
        statement.executeQuery(
            "set odps.sql.timezone=Asia/Shanghai;" +
            "select DATE'2024-09-30', DATETIME'2024-09-30 00:00:00', TIMESTAMP'2024-09-30 00:00:00.123', TIMESTAMP_NTZ'2024-09-30 00:00:00.123';");
    resultSet.next();
    for (int i = 1; i <= 4; i++) {
      Assert.assertEquals(1727625600000L, resultSet.getDate(i).getTime());
    }
  }

  @Test
  public void testTimestamp() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));

    Statement statement = conn.createStatement();
    ResultSet
        resultSet =
        statement.executeQuery(
            "set odps.sql.timezone=UTC;" +
            "select DATETIME'2024-09-30 00:00:00', TIMESTAMP'2024-09-30 00:00:00.000', TIMESTAMP_NTZ'2024-09-30 00:00:00.000';");

    resultSet.next();
    for (int i = 1; i <= 3; i++) {
      System.out.println(resultSet.getTimestamp(i).getTime());
      Assert.assertEquals(1727654400000L, resultSet.getDate(i).getTime());
    }

    resultSet =
        statement.executeQuery(
            "set odps.sql.timezone=Asia/Shanghai;" +
            "select DATETIME'2024-09-30 00:00:00', TIMESTAMP'2024-09-30 00:00:00.000', TIMESTAMP_NTZ'2024-09-30 00:00:00.000';");

    resultSet.next();
    for (int i = 1; i <= 3; i++) {
      Assert.assertEquals(1727625600000L, resultSet.getTimestamp(i).getTime());
    }
  }

  @Test
  public void testTimestampWithHighNanos() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));

    Statement statement = conn.createStatement();
    ResultSet
        resultSet =
        statement.executeQuery(
            "set odps.sql.timezone=UTC;" +
            "select TIMESTAMP'2024-09-30 00:00:00.123456789', TIMESTAMP_NTZ'2024-09-30 00:00:00.123456789';");

    resultSet.next();
    for (int i = 1; i <= 2; i++) {
      Assert.assertEquals(1727654400123L, resultSet.getTimestamp(i).getTime());
      Assert.assertEquals(123456789L, resultSet.getTimestamp(i).getNanos());
    }
  }
}
