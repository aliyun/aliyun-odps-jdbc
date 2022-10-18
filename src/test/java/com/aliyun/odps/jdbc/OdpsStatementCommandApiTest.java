package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OdpsStatementCommandApiTest {

  static final String tableName = "commandapi_test";

  static Connection conn;

  @BeforeClass
  public static void prepare() throws SQLException {
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
          DriverManager.getConnection("jdbc:odps:" + endpoint + "?project=" + pj
                                      + "&enableOdpsLogger=true&enableCommandApi=true&interactiveMode=true",
                                      accessId, accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }

    Statement statement = conn.createStatement();
    statement.execute("drop table if exists " + tableName);
    statement.execute("create table " + tableName + " (key string, value datetime);");
  }

  @AfterClass
  public static void after() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + tableName);
  }

  @Test
  public void commandApiTest() throws SQLException {
    Statement stmt = conn.createStatement();

    String sql;
    ResultSet res;

    res = stmt.executeQuery("desc " + tableName + ";");
    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }

    sql = "whoami;";
    stmt.execute(sql);
    res = stmt.getResultSet();

    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }

    sql =
        String.format("insert into %s values ('testnow' , datetime('2022-07-10 10:10:00'));",
                      tableName);
    int updateCount = stmt.executeUpdate(sql);
    System.out.println(updateCount);
    System.out.println("update sql resultSet: " + stmt.getResultSet());
    System.out.println(((OdpsStatement) stmt).getLogViewUrl());


    sql = "select * from " + tableName;
    res = stmt.executeQuery(sql);
    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }
    System.out.println(((OdpsStatement) stmt).getLogViewUrl());

  }

}
