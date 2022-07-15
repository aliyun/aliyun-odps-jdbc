package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.junit.Test;

public class OdpsJdbcClient {

  private static String driverName = "com.aliyun.odps.jdbc.OdpsDriver";

  static Connection conn;

  static {
    try {
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
              "jdbc:odps:" + endpoint + "?project=" + pj + "&enableOdpsLogger=true", accessId,
              accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    Statement stmt = conn.createStatement();
    String tableName = "newtest3";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key string, value datetime)");

    String sql;
    ResultSet res;

    // insert a record
    sql =
        String.format(
            "insert into table %s select 'time' key, '0001-01-01 00:00:00' value from (select count(1) from %s) a",
            tableName, tableName);
    System.out.println("Running: " + sql);
    int count = stmt.executeUpdate(sql);
    System.out.println("updated records: " + count);

    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getTime(2));
    }

    // regular query
    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
  }

  @Test
  public void selectTest() throws SQLException {
    Statement stmt = conn.createStatement();
    String tableName = "odps_jdbc_prepared_datetime_test";

    String sql;
    ResultSet res;

    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(
          String.valueOf(res.getString(1)) + "\t" + res.getTime(2));  // time	23:54:17
      System.out.println(
          String.valueOf(res.getString(1)) + "\t" + res.getDate(2));  // time	0001-01-02
    }
  }

  @Test
  public void preparedStatementTest() throws SQLException, ParseException {
    String tableName = "newtest";
//    Statement stmt = conn.prepareStatement("create table " + tableName + " (key string, value datetime)");
//    stmt.execute("create table " + tableName + " (key string, value datetime)");

    String sql = "INSERT INTO newtest values (?, ?)";
    OdpsPreparedStatement statement = (OdpsPreparedStatement) conn.prepareStatement(sql);
    statement.setString(1, "test6");

    String timeStr = "0001-01-01 00:00:00";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    simpleDateFormat.setCalendar(TimeTest.ISO8601_LOCAL_CALENDAR);

    statement.setDate(2,
                      new java.sql.Date(simpleDateFormat.parse("0001-01-01 00:00:00").getTime()));

//    DateTimeFormatter
//        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
//    ZonedDateTime zonedDateTime = LocalDateTime.parse("0001-01-01 00:00:00", dateTimeFormatter).atZone(ZoneId.systemDefault());
//    statement.setDate(2, new java.sql.Date(zonedDateTime.toInstant().toEpochMilli()));

    statement.execute();
    ResultSet res;

    Statement stmt = conn.createStatement();
    // select * query
    System.out.println("Running: " + sql);
    res = stmt.executeQuery("select * from " + tableName);
    while (res.next()) {
      System.out.println(
          String.valueOf(res.getString(1)) + "\t" + res.getTime(2));  // time	23:54:17
      System.out.println(
          String.valueOf(res.getString(1)) + "\t" + res.getDate(2));  // time	0001-01-02
    }
  }
}
