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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsStatementTest {

  private static Connection conn = OdpsConnectionFactory.getInstance().conn;

  @BeforeClass
  public static void setUp() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(
        "create table if not exists yichao_test_table_output(id bigint);");
    stmt.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists yichao_test_table_output;");
    stmt.close();
    conn.close();
  }

  @Test
  public void testExecuteUpdate() throws Exception {
    Statement stmt = conn.createStatement();
    String sql =
        "insert into table yichao_test_table_output select * from yichao_test_table_input;";
    int updateCount = stmt.executeUpdate(sql);
    Assert.assertEquals(100 * 10000, updateCount);
  }

  /**
   * Test the result set in the case that the lifetime of fetching rows will exceed
   * the 600 seconds which is the lifecycle of a download session.
   */
  public void testSlowlyExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement();
    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    rs.setFetchSize(10000);
    Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    Assert.assertEquals(true, rs.isBeforeFirst());
    Assert.assertEquals(10000, rs.getFetchSize());

    int i = 0;
    int j = 0;
    int time = 1;
    while (rs.next()) {
      Assert.assertEquals(i + 1, rs.getRow());
      Assert.assertEquals(i, rs.getInt(1));
      i++;
      j++;
      if (j > 9000) {
        Thread.sleep(time * 1000);
        System.out.println(String.format("%d: %ds", i, time));
        time *= 2;
      }
    }
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
    String sql = "select * from yichao_test_table_input limit 10000;";
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
    String sql = "insert into table yichao_test_table_output select * from yichao_test_table_input limit 10000;";
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

  /**
   * The result has zero rows.
   *
   * @throws Exception
   */
  @Test
  public void testExecuteQueryEmpty() throws Exception {
    Statement stmt = conn.createStatement();
    String sql = "select * from yichao_test_table_input where id < 0;";
    ResultSet rs = stmt.executeQuery(sql);

    Assert.assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(1, rsmd.getColumnCount());

    while (rs.next()) {
      System.out.println("+1");
    }
    stmt.close();
  }

  /**
   * Test the scrollability of the result set.
   * This function covers the following API of ResultSet:
   * getRow()
   * beforeFirst()
   * isBeforeFirst()
   * setFetchSize()
   * getFetchSize()
   *
   * @throws Exception
   */
  @Test
  public void testExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);

    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
    Assert.assertEquals(10000, stmt.getFetchSize());

    // Test performance for difference fetch size
    int[] fetchSizes = {
        500000,
        200000,
        100000,
        50000,
        20000,
        10000};

    int i;
    for (int fetchSize : fetchSizes) {
      long start = System.currentTimeMillis();
      rs.setFetchSize(fetchSize);
      Assert.assertEquals(fetchSize, rs.getFetchSize());
      rs.beforeFirst();
      Assert.assertEquals(true, rs.isBeforeFirst());
      {
        i = 0;
        while (rs.next()) {
          Assert.assertEquals(i + 1, rs.getRow());
          Assert.assertEquals(i, rs.getInt(1));
          i++;
        }
      }
      long end = System.currentTimeMillis();
      System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    }

    stmt.close();
  }

  @Test
  public void testExecuteMissingSemiColon() throws Exception {
    Statement stmt = conn.createStatement();

    Assert.assertEquals(true, stmt.execute("select 1 id from dual"));
    ResultSet rs = stmt.getResultSet();
    rs.next();
    Assert.assertEquals(1, rs.getInt(1));
    rs.close();

    Assert.assertEquals(true, stmt.execute("select 1 id \n,2 height\nfrom dual"));
    rs = stmt.getResultSet();
    rs.next();
    Assert.assertEquals(1, rs.getInt(1));
    rs.close();
    stmt.close();
  }

  @Test
  public void testExecuteSimple() throws Exception {
    Statement stmt = conn.createStatement();
    boolean rs_generated = stmt.execute("select 1 id, 1.5 weight from dual;");
    Assert.assertEquals(true, rs_generated);

    ResultSet rs = stmt.getResultSet();
    // Assure the result set can be generated
    Assert.assertNotNull(rs);

    rs.next();
    Assert.assertEquals(1.5, rs.getDouble(2), 0);
    Assert.assertEquals(1, rs.getInt(1));
    stmt.close();
  }

  @Test
  public void testExecuteComplex() throws Exception {
    Statement stmt = conn.createStatement();
    Assert.assertEquals(true, stmt.execute("select 1 id from dual;"));
    ResultSet rs = stmt.getResultSet();
    rs.next();
    Assert.assertEquals(1, rs.getInt(1));
    Assert.assertEquals(false, stmt.execute(
        "insert into table yichao_test_table_output select 1 id from dual;"));
    Assert.assertEquals(1, stmt.getUpdateCount());

    // do not check result
    Assert.assertEquals(true, stmt.execute(" select 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("\nselect 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("\t\r\nselect 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("SELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute(" SELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute(" SELECT 1 id--xixi\n from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\nSELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\n--hehehe\nSELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\n--hehehe\n\t \t select 1 id from dual;"));

    stmt.close();
  }


}
