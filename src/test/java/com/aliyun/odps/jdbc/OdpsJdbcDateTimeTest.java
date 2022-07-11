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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OdpsJdbcDateTimeTest {

  static final String DATETIME_TABLE_NAME = "odps_jdbc_datetime_test";
  static final String PREPARED_DATETIME_TABLE_NAME = "odps_jdbc_prepared_datetime_test";
  static final String PREPARED_DATETIME_BATCH_TABLE_NAME = "odps_jdbc_prepared_datetime_batch_test";

  static Connection conn;

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
              "jdbc:odps:" + endpoint + "?project=" + pj + "&enableOdpsLogger=true", accessId,
              accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void after() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + DATETIME_TABLE_NAME);
    stmt.execute("drop table if exists " + PREPARED_DATETIME_TABLE_NAME);
    stmt.execute("drop table if exists " + PREPARED_DATETIME_BATCH_TABLE_NAME);
  }

  @Test
  public void dateTimeTest() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + DATETIME_TABLE_NAME);
    stmt.execute("create table " + DATETIME_TABLE_NAME + " (key string, value datetime)");

    String sql;
    ResultSet res;

    // insert a record
    sql =
        String.format("insert into %s values ('test' , '0001-01-01 00:00:00');",
                      DATETIME_TABLE_NAME);
    System.out.println("Running: " + sql);
    int count = stmt.executeUpdate(sql);
    System.out.println("updated records: " + count);

    sql =
        String.format("insert into %s values ('testnow' , '2022-07-10 10:10:00');",
                      DATETIME_TABLE_NAME);
    System.out.println("Running: " + sql);
    count = stmt.executeUpdate(sql);
    System.out.println("updated records: " + count);

    sql = "select * from " + DATETIME_TABLE_NAME;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);

    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "0001-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "00:00:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "0001-01-01 00:00:00.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "2022-07-10");
      Assert.assertEquals(res.getTime(2).toString(), "10:10:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "2022-07-10 10:10:00.0");
    }
  }

  @Test
  public void preparedDateTimeBatchTest() throws SQLException, ParseException {
    String str1584 = "1584-01-01 02:00:00";
    String str0101 = "0001-01-01 00:00:00";
    String now = "2022-01-01 02:00:00";

    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + PREPARED_DATETIME_BATCH_TABLE_NAME);
    stmt.execute(
        "create table " + PREPARED_DATETIME_BATCH_TABLE_NAME + " (key string, value datetime)");

    String sql = "INSERT INTO odps_jdbc_prepared_datetime_batch_test VALUES (?, ?);";
    OdpsPreparedStatement statement = (OdpsPreparedStatement) conn.prepareStatement(sql);

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    simpleDateFormat.setCalendar(TimeTest.ISO8601_LOCAL_CALENDAR);
    statement.setString(1, "test");
    statement.setDate(2,
                      new java.sql.Date(simpleDateFormat.parse(str1584).getTime()));
    statement.addBatch();

    DateTimeFormatter
        dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    ZonedDateTime
        zonedDateTime =
        LocalDateTime.parse(str1584, dateTimeFormatter)
            .atZone(ZoneId.systemDefault());
    statement.setString(1, "zonetest");
    statement.setDate(2, new java.sql.Date(zonedDateTime.toInstant().toEpochMilli()));
    statement.addBatch();

    statement.setString(1, "test01");
    statement.setDate(2,
                      new java.sql.Date(simpleDateFormat.parse(str0101).getTime()));
    statement.addBatch();

    ZonedDateTime
        zonedDateTime2 =
        LocalDateTime.parse(str0101, dateTimeFormatter)
            .atZone(ZoneId.systemDefault());
    statement.setString(1, "zonetest01");
    statement.setDate(2, new java.sql.Date(zonedDateTime2.toInstant().toEpochMilli()));
    statement.addBatch();

    statement.setString(1, "testnow");
    statement.setDate(2,
                      new java.sql.Date(simpleDateFormat.parse(now).getTime()));
    statement.addBatch();

    statement.executeBatch();
    statement.close();

    ResultSet res;
    stmt = conn.createStatement();
    res = stmt.executeQuery("select * from " + PREPARED_DATETIME_BATCH_TABLE_NAME);

    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "1584-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "02:05:43");
      Assert.assertEquals(res.getTimestamp(2).toString(), "1584-01-01 02:05:43.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "1584-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "02:00:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "1584-01-01 02:00:00.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "0001-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "00:05:43");
      Assert.assertEquals(res.getTimestamp(2).toString(), "0001-01-01 00:05:43.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "0001-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "00:00:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "0001-01-01 00:00:00.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "2022-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "02:00:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "2022-01-01 02:00:00.0");
    }
  }

  @Test
  public void preparedDateTimeTest() throws SQLException, ParseException {
    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + PREPARED_DATETIME_TABLE_NAME);
    stmt.execute("create table " + PREPARED_DATETIME_TABLE_NAME + " (key string, value datetime)");

    ResultSet res;

    String sql = "INSERT INTO odps_jdbc_prepared_datetime_test VALUES (?, ?);";
    OdpsPreparedStatement statement = (OdpsPreparedStatement) conn.prepareStatement(sql);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    simpleDateFormat.setCalendar(TimeTest.ISO8601_LOCAL_CALENDAR);
    statement.setString(1, "test");
    statement.setDate(2,
                      new java.sql.Date(simpleDateFormat.parse("0001-01-01 00:00:00").getTime()));
    statement.execute();

    DateTimeFormatter
        dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    ZonedDateTime
        zonedDateTime =
        LocalDateTime.parse("0001-01-01 00:00:00", dateTimeFormatter)
            .atZone(ZoneId.systemDefault());
    statement.setString(1, "zonetest");
    statement.setDate(2, new java.sql.Date(zonedDateTime.toInstant().toEpochMilli()));
    statement.execute();

    sql = "select * from " + PREPARED_DATETIME_TABLE_NAME;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);

    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "0001-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "00:05:43");
      Assert.assertEquals(res.getTimestamp(2).toString(), "0001-01-01 00:05:43.0");
    }
    if (res.next()) {
      Assert.assertEquals(res.getDate(2).toString(), "0001-01-01");
      Assert.assertEquals(res.getTime(2).toString(), "00:00:00");
      Assert.assertEquals(res.getTimestamp(2).toString(), "0001-01-01 00:00:00.0");
    }
  }

}
