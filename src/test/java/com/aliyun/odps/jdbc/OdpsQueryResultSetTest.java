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
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsQueryResultSetTest {

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
  public void testSetMaxRows() throws Exception {
    Statement stmt = conn.createStatement();

    final int ROWS = 12345;
    stmt.setMaxRows(ROWS);
    Assert.assertEquals(ROWS, stmt.getMaxRows());

    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(true, rs.isBeforeFirst());

    int i = 0;
    while (rs.next()) {
      Assert.assertEquals(i, rs.getInt(1));
      i++;
    }
    Assert.assertEquals(true, rs.isAfterLast());
    Assert.assertEquals(ROWS, i);

    rs.close();
    stmt.close();
  }

  @Test
  public void testExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement();

    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(true, rs.isBeforeFirst());
    Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    Assert.assertEquals(10000, stmt.getFetchSize());

    int i = 0;
    while (rs.next()) {
      Assert.assertEquals(i + 1, rs.getRow());
      rs.getInt(1);
      i++;
    }

    Assert.assertEquals(true, rs.isAfterLast());
    rs.close();
    stmt.close();
  }

  @Test
  public void testSetFetchSize() throws Exception {
    Statement stmt = conn.createStatement();

    final int FETCH_SIZE = 12345;
    stmt.setFetchSize(FETCH_SIZE);
    Assert.assertEquals(FETCH_SIZE, stmt.getFetchSize());
    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(true, rs.isBeforeFirst());

    int i = 0;
    while (rs.next()) {
      Assert.assertEquals(i, rs.getInt(1));
      i++;
    }

    Assert.assertEquals(true, rs.isAfterLast());
    rs.close();
    stmt.close();
  }

  @Test
  public void testScollable() throws Exception {
    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);

    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

    Assert.assertEquals(true, rs.isBeforeFirst());

    int i = 0;
    while (rs.next()) {
      Assert.assertEquals(i + 1, rs.getRow());
      Assert.assertEquals(i, rs.getInt(1));
      i++;
    }

    Assert.assertEquals(true, rs.isAfterLast());

    int rows = i;
    Assert.assertEquals(1000000, rows);

    while (rs.previous()) {
      Assert.assertEquals(i, rs.getRow());
      Assert.assertEquals(i - 1, rs.getInt(1));
      i--;
    }

    Assert.assertTrue(rs.absolute(245));
    Assert.assertEquals(245, rs.getRow());
    Assert.assertEquals(244, rs.getInt(1));

    Assert.assertTrue(rs.relative(2));
    Assert.assertEquals(247, rs.getRow());
    Assert.assertEquals(246, rs.getInt(1));

    Assert.assertTrue(rs.relative(-5));
    Assert.assertEquals(242, rs.getRow());
    Assert.assertEquals(241, rs.getInt(1));

    Assert.assertFalse(rs.relative(-500));
    Assert.assertEquals(true, rs.isBeforeFirst());

    Assert.assertTrue(rs.absolute(-1));
    Assert.assertEquals(rows, rs.getRow());
    Assert.assertEquals(rows - 1, rs.getInt(1));

    Assert.assertTrue(rs.absolute(-1024));
    Assert.assertEquals(rows - 1023, rs.getRow());
    Assert.assertEquals(rows - 1024, rs.getInt(1));

    // absolute to the exact bound
    Assert.assertTrue(rs.absolute(1));
    Assert.assertEquals(true, rs.isFirst());
    Assert.assertEquals(0, rs.getInt(1));

    Assert.assertFalse(rs.relative(-1));
    Assert.assertEquals(true, rs.isBeforeFirst());

    Assert.assertTrue(rs.absolute(rows));
    Assert.assertEquals(true, rs.isLast());
    Assert.assertEquals(rows - 1, rs.getInt(1));

    Assert.assertTrue(rs.absolute(-rows));
    Assert.assertEquals(true, rs.isFirst());
    Assert.assertEquals(0, rs.getInt(1));

    // absolute out of bound
    Assert.assertFalse(rs.absolute(0));
    Assert.assertEquals(true, rs.isBeforeFirst());

    Assert.assertFalse(rs.absolute(rows + 1));
    Assert.assertEquals(true, rs.isAfterLast());

    Assert.assertFalse(rs.relative(1));
    Assert.assertEquals(true, rs.isAfterLast());

    Assert.assertFalse(rs.absolute(-rows - 1));
    Assert.assertEquals(true, rs.isBeforeFirst());

    rs.close();
    stmt.close();
  }
}
