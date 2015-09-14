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

  private static Connection conn;
  private static Statement stmt;
  private static ResultSet rs;
  private static final String SQL = "select * from yichao_test_table_input;";
  private static final int ROWS = 1000000;

  @BeforeClass
  public static void setUp() throws Exception {
    conn = OdpsConnectionFactory.getInstance().conn;
    stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);
    rs = stmt.executeQuery(SQL);
    Assert.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    rs.close();
    stmt.close();
    conn.close();
  }

  @Test
  public void testGetType() throws Exception {
    Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
  }

  @Test
  public void testSetFetchDirection() throws Exception {

    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    Assert.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

    rs.setFetchDirection(ResultSet.FETCH_REVERSE);
    Assert.assertEquals(ResultSet.FETCH_REVERSE, rs.getFetchDirection());

    rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    Assert.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
  }

  @Test
  public void testSetFetchSize() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(12345);
    Assert.assertEquals(12345, rs.getFetchSize());
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
    }
    Assert.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testBorderCase() throws Exception {
    rs.afterLast();
    Assert.assertEquals(true, rs.isAfterLast());

    rs.beforeFirst();
    Assert.assertEquals(true, rs.isBeforeFirst());

    rs.first();
    Assert.assertEquals(true, rs.isFirst());

    rs.last();
    Assert.assertEquals(true, rs.isLast());
  }

  @Test
  public void testReverse10K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_REVERSE);
    rs.setFetchSize(10000);
    {
      int i = ROWS;
      while (rs.previous()) {
        Assert.assertEquals(i, rs.getRow());
        Assert.assertEquals(i - 1, rs.getInt(1));
        i--;
      }
      Assert.assertEquals(0, i);
    }
    Assert.assertEquals(true, rs.isBeforeFirst());
  }

  @Test
  public void testForward10K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(100000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assert.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assert.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testForward100K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(100000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assert.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assert.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testForward5K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(5000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assert.assertEquals(i + 1, rs.getRow());
        Assert.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assert.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assert.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testRandomAccess() throws Exception {
    rs.setFetchSize(5000);

    // free walk
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
    Assert.assertEquals(ROWS, rs.getRow());
    Assert.assertEquals(ROWS - 1, rs.getInt(1));

    Assert.assertTrue(rs.absolute(-1024));
    Assert.assertEquals(ROWS - 1023, rs.getRow());
    Assert.assertEquals(ROWS - 1024, rs.getInt(1));

    // absolute to the exact bound
    Assert.assertTrue(rs.absolute(1));
    Assert.assertEquals(true, rs.isFirst());
    Assert.assertEquals(0, rs.getInt(1));

    Assert.assertFalse(rs.relative(-1));
    Assert.assertEquals(true, rs.isBeforeFirst());

    Assert.assertTrue(rs.absolute(ROWS));
    Assert.assertEquals(true, rs.isLast());
    Assert.assertEquals(ROWS - 1, rs.getInt(1));

    Assert.assertTrue(rs.absolute(-ROWS));
    Assert.assertEquals(true, rs.isFirst());
    Assert.assertEquals(0, rs.getInt(1));

    // absolute out of bound
    Assert.assertFalse(rs.absolute(0));
    Assert.assertEquals(true, rs.isBeforeFirst());

    Assert.assertFalse(rs.absolute(ROWS + 1));
    Assert.assertEquals(true, rs.isAfterLast());

    Assert.assertFalse(rs.relative(1));
    Assert.assertEquals(true, rs.isAfterLast());

    Assert.assertFalse(rs.absolute(-ROWS - 1));
    Assert.assertEquals(true, rs.isBeforeFirst());
  }
}