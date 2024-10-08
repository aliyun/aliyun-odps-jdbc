package com.aliyun.odps.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OdpsJdbcDateTest {

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
              "jdbc:odps:" + endpoint + "?project=" + pj + "&enableOdpsLogger=true&useProjectTimeZone=true&timezone=UTC", accessId,
              accessKey);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * 2024-0930 UTC
   * 1727625600000 at UTC
   * 1727654400000 at Asia/Shanghai
   */
  @Test
  public void testTimezone() {
    // mock jdbc timezone is Asia/Shanghai
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    try {
      ResultSet
          resultSet =
          conn.createStatement().executeQuery("SELECT `tableausql`.`t1` AS `t1`,\n"
                                              + "  `tableausql`.`t2` AS `t2`,\n"
                                              + "  `tableausql`.`t3` AS `t3`\n"
                                              + "FROM (\n"
                                              + "  select\n"
                                              + "  '2024-09-30' t1\n"
                                              + "  ,to_date('2024-09-30') t2\n"
                                              + "  ,date(to_date('2024-09-30 23:59:59') ) t3\n"
                                              + ") `tableausql`\n"
                                              + "LIMIT 200;");
      resultSet.next();
      Object date1 = resultSet.getObject(2);
      Object date2 = resultSet.getObject(3);

      LocalDate localDate = (LocalDate) date1;
      Date origin = Date.valueOf(localDate);
      Date afterBugfix = resultSet.getDate(2);

      Assert.assertEquals(date1, date2);
      Assert.assertNotEquals(origin, afterBugfix);

      // mock client timezone is utc
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      System.out.println(afterBugfix);
      System.out.println(origin);

      Assert.assertEquals("2024-09-30", afterBugfix.toString());
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

}
