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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsPreparedStatementTest {

  static PreparedStatement pstmt;
  static long unixtime;

  @BeforeClass
  public static void setUp() throws Exception {
    pstmt = OdpsConnectionFactory.getInstance().conn.prepareStatement(
        "select ? whatever from dual;");
    unixtime = new java.util.Date().getTime();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    pstmt.close();
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testSetBigDecimal() throws Exception {
    BigDecimal x = BigDecimal.TEN;
    pstmt.setBigDecimal(1, x);

    ResultSet rs = pstmt.executeQuery();

    BigDecimal y = BigDecimal.ONE;
    while (rs.next()) {
      y = rs.getBigDecimal(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetBoolean() throws Exception {
    pstmt.setBoolean(1, Boolean.TRUE);

    ResultSet rs = pstmt.executeQuery();

    Boolean y = Boolean.FALSE;
    while (rs.next()) {
      y = rs.getBoolean(1);
    }
    Assert.assertEquals(Boolean.TRUE, y);
  }

  @Test
  public void testSetByte() throws Exception {
    byte x = Byte.MAX_VALUE;
    pstmt.setByte(1, x);

    ResultSet rs = pstmt.executeQuery();

    byte y = 0;
    while (rs.next()) {
      y = rs.getByte(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetDate() throws Exception {
    Date x = new Date(unixtime);
    pstmt.setDate(1, x);

    ResultSet rs = pstmt.executeQuery();

    Date y = new Date(0);
    while (rs.next()) {
      y = rs.getDate(1);
    }

    Assert.assertEquals(x.toString(), y.toString());
  }

  @Test
  public void testSetDouble() throws Exception {
    double x = Double.MAX_VALUE;
    pstmt.setDouble(1, x);

    ResultSet rs = pstmt.executeQuery();

    double y = 0.0E00;
    while (rs.next()) {
      y = rs.getDouble(1);
    }
    Assert.assertEquals(x, y, 0);
  }

  @Test
  public void testSetFloat() throws Exception {
    float x = Float.MAX_VALUE;
    pstmt.setFloat(1, x);

    ResultSet rs = pstmt.executeQuery();

    float y = 0;
    while (rs.next()) {
      y = rs.getFloat(1);
    }
    Assert.assertEquals(x, y, 0);
  }

  @Test
  public void testSetInt() throws Exception {
    int x = Integer.MAX_VALUE;
    pstmt.setInt(1, x);

    ResultSet rs = pstmt.executeQuery();

    int y = 0;
    while (rs.next()) {
      y = rs.getInt(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetNull() throws Exception {
    PreparedStatement pstmt2 = OdpsConnectionFactory.getInstance().conn.prepareStatement(
        "select * from (select 1 id from dual) x where id = ?;");

    pstmt2.setInt(1, 1);
    ResultSet rs = pstmt2.executeQuery();

    while (rs.next()) {
      Assert.assertEquals(1, rs.getInt(1));
    }

    // Set it to null
    pstmt2.setNull(1, Types.BIGINT);
    rs = pstmt2.executeQuery();

    // The value of null integer has a literal value of 0
    // But the invocation to wasNull() will return true.
    while (rs.next()) {
      Assert.assertEquals(0, rs.getInt(1));
      Assert.assertTrue(rs.wasNull());
    }
    pstmt2.close();
  }

  @Test
  public void testSetObject() throws Exception {
    Double x = 0.314;
    pstmt.setObject(1, x);

    ResultSet rs = pstmt.executeQuery();

    double y = 0;
    while (rs.next()) {
      y = rs.getDouble(1);
    }
    Assert.assertEquals(x.doubleValue(), y, 0);
  }

  @Test(expected = SQLException.class)
  public void testSetUndefinedObject() throws Exception {
    BigInteger x = new BigInteger("123");
    pstmt.setObject(1, x);
  }

  @Test
  public void testSetLong() throws Exception {
    long x = Long.MAX_VALUE;
    pstmt.setLong(1, x);

    ResultSet rs = pstmt.executeQuery();

    long y = 0;
    while (rs.next()) {
      y = rs.getLong(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetShort() throws Exception {
    short x = Short.MAX_VALUE;
    pstmt.setShort(1, x);

    ResultSet rs = pstmt.executeQuery();

    short y = 0;
    while (rs.next()) {
      y = rs.getShort(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetString() throws Exception {
    String x = "hello";
    pstmt.setString(1, x);

    ResultSet rs = pstmt.executeQuery();

    String y = "";
    while (rs.next()) {
      y = rs.getString(1);
    }
    Assert.assertEquals(x, y);
  }

  @Test
  public void testSetTime() throws Exception {
    Time x = new Time(unixtime);
    pstmt.setTime(1, x);

    ResultSet rs = pstmt.executeQuery();

    Time y = new Time(0);
    while (rs.next()) {
      y = rs.getTime(1);
    }
    Assert.assertEquals(x.toString(), y.toString());
  }

  @Test
  public void testSetTimestamp() throws Exception {
    Timestamp x = new Timestamp(unixtime);
    pstmt.setTimestamp(1, x);

    ResultSet rs = pstmt.executeQuery();

    Timestamp y = new Timestamp(0);
    while (rs.next()) {
      y = rs.getTimestamp(1);
    }

    // Walk around the precision problem
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String ys = formatter.format(y);
    String xs = formatter.format(x);

    Assert.assertEquals(xs, ys);
  }
}

