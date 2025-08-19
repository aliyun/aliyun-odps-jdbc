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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsSessionScollResultSetTest {

  private static Connection conn;
  private static Connection sessionConn;
  private static Statement stmt;
  private static ResultSet rs;
  private static Odps odps;
  private static TableTunnel tunnel;
  private static String INPUT_TABLE_NAME = "statement_test_table_input";
  private static final String
      SQL =
      "set odps.sql.select.auto.limit=-1;set odps.sql.session.result.cache.enable=false;select * from "
      + INPUT_TABLE_NAME;
  private static final int ROWS = 100000;

  @BeforeAll
  public static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    sessionConn = TestUtils.getConnection(com.google.common.collect.ImmutableMap.of("interactiveMode", "true", "enableCommandApi", "true"));
    OdpsConnection odpsConn = (OdpsConnection) sessionConn;
    odpsConn.setEnableLimit(false);
    odps = TestUtils.getOdps();
    tunnel = new TableTunnel(odps);
    
    stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("create table if not exists " + INPUT_TABLE_NAME + "(id bigint);");

    TableTunnel.UploadSession upload = tunnel.createUploadSession(odps.getDefaultProject(), INPUT_TABLE_NAME);

    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    for (int i = 0; i < ROWS; i++) {
      r.setBigint(0, (long) i);
      writer.write(r);
    }
    writer.close();
    upload.commit(new Long[]{0L});

    stmt = sessionConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY);
    rs = stmt.executeQuery(SQL);
    Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    rs.close();
    stmt.close();
  }

  @Test
  public void testGetType() throws Exception {
    Assertions.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
  }

  @Test
  public void testSetFetchDirection() throws Exception {

    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    Assertions.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

    rs.setFetchDirection(ResultSet.FETCH_REVERSE);
    Assertions.assertEquals(ResultSet.FETCH_REVERSE, rs.getFetchDirection());

    rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    Assertions.assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
  }

  @Test
  public void testSetFetchSize() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(12345);
    Assertions.assertEquals(12345, rs.getFetchSize());
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
    }
    Assertions.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testBorderCase() throws Exception {
    rs.afterLast();
    Assertions.assertEquals(true, rs.isAfterLast());

    rs.beforeFirst();
    Assertions.assertEquals(true, rs.isBeforeFirst());

    rs.first();
    Assertions.assertEquals(true, rs.isFirst());

    rs.last();
    Assertions.assertEquals(true, rs.isLast());
  }

  @Test
  public void testReverse10K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_REVERSE);
    rs.setFetchSize(10000);
    {
      int i = ROWS;
      while (rs.previous()) {
        Assertions.assertEquals(i, rs.getRow());
        Assertions.assertEquals(i - 1, rs.getInt(1));
        i--;
      }
      Assertions.assertEquals(0, i);
    }
    Assertions.assertEquals(true, rs.isBeforeFirst());
  }

  @Test
  public void testForward10K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(100000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assertions.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assertions.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testForward100K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(100000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assertions.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assertions.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testForward5K() throws Exception {
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    rs.setFetchSize(5000);
    long start = System.currentTimeMillis();
    {
      int i = 0;
      while (rs.next()) {
        Assertions.assertEquals(i + 1, rs.getRow());
        Assertions.assertEquals(i, rs.getInt(1));
        i++;
      }
      Assertions.assertEquals(ROWS, i);
    }
    long end = System.currentTimeMillis();
    System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    Assertions.assertEquals(true, rs.isAfterLast());
  }

  @Test
  public void testRandomAccess() throws Exception {
    rs.setFetchSize(5000);

    // free walk
    Assertions.assertTrue(rs.absolute(245));
    Assertions.assertEquals(245, rs.getRow());
    Assertions.assertEquals(244, rs.getInt(1));

    Assertions.assertTrue(rs.relative(2));
    Assertions.assertEquals(247, rs.getRow());
    Assertions.assertEquals(246, rs.getInt(1));

    Assertions.assertTrue(rs.relative(-5));
    Assertions.assertEquals(242, rs.getRow());
    Assertions.assertEquals(241, rs.getInt(1));

    Assertions.assertFalse(rs.relative(-500));
    Assertions.assertEquals(true, rs.isBeforeFirst());

    Assertions.assertTrue(rs.absolute(-1));
    Assertions.assertEquals(ROWS, rs.getRow());
    Assertions.assertEquals(ROWS - 1, rs.getInt(1));

    Assertions.assertTrue(rs.absolute(-1024));
    Assertions.assertEquals(ROWS - 1023, rs.getRow());
    Assertions.assertEquals(ROWS - 1024, rs.getInt(1));

    // absolute to the exact bound
    Assertions.assertTrue(rs.absolute(1));
    Assertions.assertEquals(true, rs.isFirst());
    Assertions.assertEquals(0, rs.getInt(1));

    Assertions.assertFalse(rs.relative(-1));
    Assertions.assertEquals(true, rs.isBeforeFirst());

    Assertions.assertTrue(rs.absolute(ROWS));
    Assertions.assertEquals(true, rs.isLast());
    Assertions.assertEquals(ROWS - 1, rs.getInt(1));

    Assertions.assertTrue(rs.absolute(-ROWS));
    Assertions.assertEquals(true, rs.isFirst());
    Assertions.assertEquals(0, rs.getInt(1));

    // absolute out of bound
    Assertions.assertFalse(rs.absolute(0));
    Assertions.assertEquals(true, rs.isBeforeFirst());

    Assertions.assertFalse(rs.absolute(ROWS + 1));
    Assertions.assertEquals(true, rs.isAfterLast());

    Assertions.assertFalse(rs.relative(1));
    Assertions.assertEquals(true, rs.isAfterLast());

    Assertions.assertFalse(rs.absolute(-ROWS - 1));
    Assertions.assertEquals(true, rs.isBeforeFirst());
  }
}