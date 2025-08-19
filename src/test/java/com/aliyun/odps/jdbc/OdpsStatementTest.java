/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

public class OdpsStatementTest {

  private static Connection conn;
  private static Odps odps;
  private static TableTunnel tunnel;
  private static final int ROWS = 100000;

  private static String OUTPUT_TABLE_NAME = "statement_test_table_output";
  private static String INPUT_TABLE_NAME = "statement_test_table_input";
  private static String PARTITIONED_TABLE_NAME = "partitioned_table_name";

  @BeforeAll
  public static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    odps = TestUtils.getOdps();
    tunnel = new TableTunnel(odps);
    
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + PARTITIONED_TABLE_NAME);
    stmt.executeUpdate("create table if not exists " + INPUT_TABLE_NAME + "(id bigint);");
    stmt.executeUpdate("create table if not exists " + OUTPUT_TABLE_NAME + "(id bigint);");
    stmt.executeUpdate("create table if not exists " + PARTITIONED_TABLE_NAME
                       + "(foo bigint) partitioned by (bar string);");
    stmt.executeUpdate("alter table " + PARTITIONED_TABLE_NAME + " add partition (bar='hello')");
    stmt.close();

    TableTunnel.UploadSession upload = tunnel.createUploadSession(
        odps.getDefaultProject(), INPUT_TABLE_NAME);

    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    for (int i = 0; i < ROWS; i++) {
      r.setBigint(0, (long) i);
      writer.write(r);
    }
    writer.close();
    upload.commit(new Long[]{0L});
  }

  @AfterAll
  public static void tearDown() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.close();
  }

  @Test
  public void testExecuteSet() throws Exception {
    Statement stmt = conn.createStatement();
    Assertions.assertFalse(stmt.execute("   set  sql.x.y = 123 ;"));
    Assertions.assertFalse(stmt.execute("   set  sql.a.b=  1111"));
    Assertions.assertFalse(stmt.execute("SET  sql.c.d =abcdefgh"));
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    stmt.close();
  }

  @Test
  public void testExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    Assertions.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());

    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\tmillis\t%d\n", end - start);
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    stmt.close();
    Assertions.assertTrue(stmt.isClosed());
  }

  @Test
  public void testSelectNullQuery() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select null from " + INPUT_TABLE_NAME);
    Assertions.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());

    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(null, rs.getObject(1));
        Assertions.assertEquals(null, rs.getString(1));
        i++;
      }
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\tmillis\t%d\n", end - start);
    rs.close();
    Assertions.assertTrue(rs.isClosed());
    stmt.close();
    Assertions.assertTrue(stmt.isClosed());
  }

  @Test
  public void testSetMaxRows() throws Exception {
    Connection conn = TestUtils.getConnection(ImmutableMap.of("enableLimit", "false"));
    Statement stmt = conn.createStatement();
    stmt.setMaxRows(45678);
    Assertions.assertEquals(45678, stmt.getMaxRows());
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assertions.assertEquals(45678, i);
    }
    rs.close();
    stmt.close();
  }

  @Test
  public void testExecuteUpdate() throws Exception {
    Statement stmt = conn.createStatement();
    String sql =
        "insert into table " + OUTPUT_TABLE_NAME + " select * from " + INPUT_TABLE_NAME;
    int updateCount = stmt.executeUpdate(sql);
    Assertions.assertEquals(ROWS, updateCount);
    stmt.close();
  }

  /**
   * Thread for a sql to be cancelled
   */
  class ExecuteSQL implements Runnable {

    Statement stmt;
    public Thread mythread;
    String sql;

    ExecuteSQL(Statement stmt, String sql) {
      this.stmt = stmt;
      this.sql = sql;
      mythread = new Thread(this, "sql thread");
      System.out.println("thread created: " + mythread);
    }

    public void run() {
      try {
        System.out.println("trigger sql");
        boolean kind = stmt.execute(sql);
        System.out.println(kind ? "query ok" : "update ok");
      } catch (SQLException e) {
        System.out.println("run sql fail: " + e.getMessage());
      }
    }
  }

  /**
   * Thread class for cancelling sql
   */
  class CancelSQL implements Runnable {

    Statement stmt;
    public Thread mythread;

    CancelSQL(Statement stmt) {
      this.stmt = stmt;
      mythread = new Thread(this, "cancel thread");
      System.out.println("thread created: " + mythread);
    }

    public void run() {
      try {
        System.out.println("trigger cancel");
        stmt.cancel();
      } catch (SQLException e) {
        System.out.println("cancel fail: " + e.getMessage());
      }
    }
  }

  @Test
  public void testCancelQuery() throws Exception {
    Statement stmt = conn.createStatement();
    String sql = "select * from " + INPUT_TABLE_NAME + " limit 10000;";
    ExecuteSQL updateIt = new ExecuteSQL(stmt, sql);
    CancelSQL cancelIt = new CancelSQL(stmt);

    // kicks-off execution 4s earlier
    updateIt.mythread.start();
    Thread.sleep(4000);
    cancelIt.mythread.start();

    updateIt.mythread.join();
    cancelIt.mythread.join();
    stmt.close();
  }

  @Test
  public void testCancelUpdate() throws Exception {
    Statement stmt = conn.createStatement();
    String
        sql =
        "insert into table " + OUTPUT_TABLE_NAME + " select * from " + INPUT_TABLE_NAME
        + " limit 10000;";
    ExecuteSQL updateIt = new ExecuteSQL(stmt, sql);
    CancelSQL cancelIt = new CancelSQL(stmt);

    // kicks-off execution 4s earlier
    updateIt.mythread.start();
    Thread.sleep(4000);
    cancelIt.mythread.start();

    updateIt.mythread.join();
    cancelIt.mythread.join();
    stmt.close();
  }

  @Test
  public void testExecuteQueryEmpty() throws Exception {
    Statement stmt = conn.createStatement();
    String sql = "select * from " + INPUT_TABLE_NAME + " where id < 0;";
    ResultSet rs = stmt.executeQuery(sql);

    Assertions.assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();
    Assertions.assertEquals(1, rsmd.getColumnCount());

    stmt.close();
  }

  @Test
  public void testExecuteMissingSemiColon() throws Exception {
    Statement stmt = conn.createStatement();

    Assertions.assertEquals(true, stmt.execute("select 1 id from " + INPUT_TABLE_NAME + " limit 1;"));
    ResultSet rs = stmt.getResultSet();
    {
      rs.next();
      Assertions.assertEquals(1, rs.getInt(1));
    }

    Assertions.assertEquals(true, stmt.execute(
        "select 1 id \n,2 height\nfrom " + INPUT_TABLE_NAME + " limit 1;"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assertions.assertEquals(1, rs.getInt(1));
    }

    Assertions.assertEquals(true, stmt.execute("select 1 id from " + INPUT_TABLE_NAME + " limit 1"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assertions.assertEquals(1, rs.getInt(1));
    }

    Assertions.assertEquals(true, stmt.execute(
        "select 1 id \n,2 height\nfrom " + INPUT_TABLE_NAME + " limit 1"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assertions.assertEquals(1, rs.getInt(1));
    }

    rs.close();
    stmt.close();
  }

  @Test
  public void testQueryOrUpdate() throws Exception {
    Assertions.assertTrue(OdpsStatement.isQuery("select 1 id, 1.5 weight from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery(" select 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("\nselect 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("\t\r\nselect 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("SELECT 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery(" SELECT 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery(" SELECT 1 id--xixi\n from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("--abcd\nSELECT 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("--abcd\n--hehehe\nSELECT 1 id from dual;"));
    Assertions.assertTrue(OdpsStatement.isQuery("--abcd\n--hehehe\n\t \t select 1 id from dual;"));
    Assertions.assertFalse(
        OdpsStatement.isQuery("insert into table yichao_test_table_output select 1 id from dual;"));
    Assertions.assertFalse(OdpsStatement.isQuery(
        "insert into table\nyichao_test_table_output\nselect 1 id from dual;"));
  }

  @Test
  public void testDescTable() {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("desc " + PARTITIONED_TABLE_NAME);
      try (ResultSet rs = stmt.getResultSet()) {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(rs.getString(i));
            if (i == columnCount) {
              System.out.print("\n");
            } else {
              System.out.print(", ");
            }
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDescPartition() {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("desc " + PARTITIONED_TABLE_NAME + " partition (bar='hello');");
      try (ResultSet rs = stmt.getResultSet()) {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(rs.getString(i));
            if (i == columnCount) {
              System.out.print("\n");
            } else {
              System.out.print(", ");
            }
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTables() {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("show tables;");
      try (ResultSet rs = stmt.getResultSet()) {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(rs.getString(i));
            if (i == columnCount) {
              System.out.print("\n");
            } else {
              System.out.print(", ");
            }
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowPartitions() {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("show partitions " + PARTITIONED_TABLE_NAME);
      try (ResultSet rs = stmt.getResultSet()) {
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(rs.getString(i));
            if (i == columnCount) {
              System.out.print("\n");
            } else {
              System.out.print(", ");
            }
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSetTimeZone() throws SQLException {
    // 先指定 calendar 的时区再设置时间
    Calendar shanghaiCal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
    shanghaiCal.set(2020, Calendar.JANUARY, 1, 0, 0, 0);
    long localTimestampSecond = shanghaiCal.toInstant().getEpochSecond();
    System.out.println("Shanghai 2020-01-01T00:00 epoch seconds: " + localTimestampSecond);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET odps.sql.timezone=UTC");

      try (ResultSet rs = stmt.executeQuery(
          "SELECT CAST('2020-01-01 00:00:00' AS DATETIME)")) {

        while (rs.next()) {
          // 按 UTC 解析 DB 值
          Timestamp utcTs = rs.getTimestamp(1);
          long utcTimestampSecond = utcTs.getTime() / 1000;
          // 本地时间戳 - UTC 时间戳 == 时区偏移秒数
          Assertions.assertEquals(localTimestampSecond - utcTimestampSecond, 0);
        }
      }
    }

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT CAST('2020-01-01 00:00:00' AS DATETIME)")) {

        while (rs.next()) {
          // 按 UTC 解析 DB 值
          Timestamp utcTs = rs.getTimestamp(1);
          long utcTimestampSecond = utcTs.getTime() / 1000;
          System.out.println("UTC 2020-01-01T00:00 epoch seconds: " + utcTimestampSecond);

          long offsetSeconds = TimeZone.getDefault().getOffset(utcTs.getTime()) / 1000;
          // 本地时间戳 - UTC 时间戳 == 时区偏移秒数
          Assertions.assertEquals(localTimestampSecond - utcTimestampSecond, offsetSeconds);
        }
      }
    }
  }
}