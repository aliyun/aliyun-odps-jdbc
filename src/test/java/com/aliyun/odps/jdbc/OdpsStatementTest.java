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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsStatementTest {

  private static Connection conn = TestManager.getInstance().conn;
  private static final int ROWS = 100000;

  private static String OUTPUT_TABLE_NAME = "statement_test_table_output";
  private static String INPUT_TABLE_NAME = "statement_test_table_input";
  private static String PARTITIONED_TABLE_NAME = "partitioned_table_name";

  @BeforeClass
  public static void setUp() throws Exception {
    Statement stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + PARTITIONED_TABLE_NAME);
    stmt.executeUpdate("create table if not exists " + INPUT_TABLE_NAME + "(id bigint);");
    stmt.executeUpdate("create table if not exists " + OUTPUT_TABLE_NAME + "(id bigint);");
    stmt.executeUpdate("create table if not exists " + PARTITIONED_TABLE_NAME + "(foo bigint) partitioned by (bar string);");
    stmt.executeUpdate("alter table " + PARTITIONED_TABLE_NAME + " add partition (bar='hello')");
    stmt.close();

    TableTunnel.UploadSession upload = TestManager.getInstance().tunnel.createUploadSession(
        TestManager.getInstance().odps.getDefaultProject(), INPUT_TABLE_NAME);

    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    for (int i = 0; i < ROWS; i++) {
      r.setBigint(0, (long) i);
      writer.write(r);
    }
    writer.close();
    upload.commit(new Long[]{0L});
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.close();
  }

  @Test
  public void testExecuteSet() throws Exception {
    Statement stmt = conn.createStatement();
    Assert.assertFalse(stmt.execute("   set  sql.x.y = 123 ;"));
    Assert.assertFalse(stmt.execute("   set  sql.a.b=  1111"));
    Assert.assertFalse(stmt.execute("SET  sql.c.d =abcdefgh"));
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    stmt.close();
  }

  @Test
  public void testExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());

    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\tmillis\t%d\n", end - start);
    rs.close();
    Assert.assertTrue(rs.isClosed());
    stmt.close();
    Assert.assertTrue(stmt.isClosed());
  }
  
  @Test
  public void testSelectNullQuery() throws Exception {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select null from " + INPUT_TABLE_NAME);
    Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());

    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(null, rs.getObject(1));
        Assert.assertEquals(null, rs.getString(1));
        i++;
      }
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\tmillis\t%d\n", end - start);
    rs.close();
    Assert.assertTrue(rs.isClosed());
    stmt.close();
    Assert.assertTrue(stmt.isClosed());
  }

  @Test
  public void testSetMaxRows() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.setMaxRows(45678);
    Assert.assertEquals(45678, stmt.getMaxRows());
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME);
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assert.assertEquals(45678, i);
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
    Assert.assertEquals(ROWS, updateCount);
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

    Assert.assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(1, rsmd.getColumnCount());

    stmt.close();
  }

  @Test
  public void testExecuteMissingSemiColon() throws Exception {
    Statement stmt = conn.createStatement();

    Assert.assertEquals(true, stmt.execute("select 1 id from " + INPUT_TABLE_NAME + " limit 1;"));
    ResultSet rs = stmt.getResultSet();
    {
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
    }

    Assert.assertEquals(true, stmt.execute(
        "select 1 id \n,2 height\nfrom " + INPUT_TABLE_NAME + " limit 1;"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
    }

    Assert.assertEquals(true, stmt.execute("select 1 id from " + INPUT_TABLE_NAME + " limit 1"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
    }

    Assert.assertEquals(true, stmt.execute(
        "select 1 id \n,2 height\nfrom " + INPUT_TABLE_NAME + " limit 1"));
    rs = stmt.getResultSet();
    {
      rs.next();
      Assert.assertEquals(1, rs.getInt(1));
    }

    rs.close();
    stmt.close();
  }

  @Test
  public void testQueryOrUpdate() throws Exception {
    Assert.assertTrue(OdpsStatement.isQuery("select 1 id, 1.5 weight from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery(" select 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("\nselect 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("\t\r\nselect 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("SELECT 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery(" SELECT 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery(" SELECT 1 id--xixi\n from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("--abcd\nSELECT 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("--abcd\n--hehehe\nSELECT 1 id from dual;"));
    Assert.assertTrue(OdpsStatement.isQuery("--abcd\n--hehehe\n\t \t select 1 id from dual;"));
    Assert.assertFalse(
        OdpsStatement.isQuery("insert into table yichao_test_table_output select 1 id from dual;"));
    Assert.assertFalse(OdpsStatement.isQuery(
        "insert into table\nyichao_test_table_output\nselect 1 id from dual;"));
  }

  @Test
  public void testDescTable() {
    try (Statement stmt = TestManager.getInstance().conn.createStatement()) {
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
    try (Statement stmt = TestManager.getInstance().conn.createStatement()) {
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
    try (Statement stmt = TestManager.getInstance().conn.createStatement()) {
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
    try (Statement stmt = TestManager.getInstance().conn.createStatement()) {
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
}
