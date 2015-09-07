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
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Formatter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsResultSetTest {

  static Statement stmt;

  static long unixTimeNow;
  static SimpleDateFormat formatter;
  static String nowStr;
  static String odpsNowStr;

  static String decimalStr;
  static String odpsDecimalStr;
  static BigDecimal bigDecimal;

  @BeforeClass
  public static void setUp() throws Exception {
    stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    unixTimeNow = new java.util.Date().getTime();
    nowStr = formatter.format(unixTimeNow);
    odpsNowStr = "cast('" + nowStr + "' as datetime)";

    decimalStr = "55.123456789012345";
    odpsDecimalStr = "cast('" + decimalStr + "' as decimal)";
    bigDecimal = new BigDecimal(decimalStr);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    stmt.close();
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testGetObject() throws Exception {
    ResultSet
        rs = stmt.executeQuery("select * from (select 1 id, 1.5 weight from dual" +
                               " union all select 2 id, 2.9 weight from dual) x;");
    {
      rs.next();
      Assert.assertEquals(1, ((Long) rs.getObject(1)).longValue());
      Assert.assertEquals(1, ((Long) rs.getObject("id")).longValue());
      Assert.assertEquals(1.5, ((Double) rs.getObject(2)).doubleValue(), 0);
      Assert.assertEquals(1.5, ((Double) rs.getObject("weight")).doubleValue(), 0);
    }

    {
      rs.next();
      Assert.assertEquals(2, ((Long) rs.getObject(1)).longValue());
      Assert.assertEquals(2, ((Long) rs.getObject("id")).longValue());
      Assert.assertEquals(2.9, ((Double) rs.getObject(2)).doubleValue(), 0);
      Assert.assertEquals(2.9, ((Double) rs.getObject("weight")).doubleValue(), 0);
    }

    rs.close();
  }

  @Test
  public void testGetBoolean() throws Exception {
    // cast from BOOLEAN, STRING, DOUBLE, BIGINT
    ResultSet rs = stmt.executeQuery("select true c1, false c2, '42' c3, '0' c4, "
                                     + "3.14 c5, 0.0 c6, 95 c7, 0 c8 from dual;");
    {
      rs.next();
      Assert.assertEquals(true, rs.getBoolean(1));
      Assert.assertEquals(false, rs.getBoolean(2));
      Assert.assertEquals(true, rs.getBoolean(3));
      Assert.assertEquals(false, rs.getBoolean(4));
      Assert.assertEquals(true, rs.getBoolean(5));
      Assert.assertEquals(false, rs.getBoolean(6));
      Assert.assertEquals(true, rs.getBoolean(7));
      Assert.assertEquals(false, rs.getBoolean(8));
    }
    rs.close();
  }

  @Test
  public void testGetByte() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL
    ResultSet rs = stmt.executeQuery(
        String.format("select 1943 c1, 3.1415926 c2, %s c3 from dual;", odpsDecimalStr));
    {
      rs.next();
      Assert.assertEquals((byte) 1943, rs.getByte(1));
      Assert.assertEquals((byte) 3.1415926, rs.getByte(2));
      Assert.assertEquals(bigDecimal.byteValue(), rs.getByte(3));
    }
    rs.close();
  }

  @Test
  public void testGetInteger() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    ResultSet rs = stmt.executeQuery(
        String.format("select 1943 c1, 3.1415926 c2, %s c3, '1234' c4 from dual;", odpsDecimalStr));
    {
      rs.next();
      Assert.assertEquals(1943, rs.getInt(1));
      Assert.assertEquals((int) 3.1415926, rs.getInt(2));
      Assert.assertEquals(bigDecimal.intValue(), rs.getInt(3));
      Assert.assertEquals(1234, rs.getInt(4));

      Assert.assertEquals((short) 1943, rs.getShort(1));
      Assert.assertEquals((short) 3.1415926, rs.getShort(2));
      Assert.assertEquals(bigDecimal.shortValue(), rs.getShort(3));
      Assert.assertEquals((short) 1234, rs.getShort(4));

      Assert.assertEquals((long) 1943, rs.getLong(1));
      Assert.assertEquals((long) 3.1415926, rs.getLong(2));
      Assert.assertEquals(bigDecimal.longValue(), rs.getLong(3));
      Assert.assertEquals((long) 1234, rs.getLong(4));
    }
    rs.close();
  }

  @Test
  public void testGetFloatPoint() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    ResultSet rs = stmt.executeQuery(
        String.format("select 1943 c1, 3.1415926 c2, %s c3, '3.1415926' c4 from dual;",
                      odpsDecimalStr));
    {
      rs.next();
      Assert.assertEquals((double) 1943, rs.getDouble(1), 0);
      Assert.assertEquals(3.1415926, rs.getDouble(2), 0);
      Assert.assertEquals(bigDecimal.doubleValue(), rs.getDouble(3), 0);
      Assert.assertEquals(3.1415926, rs.getDouble(4), 0);

      Assert.assertEquals((float) 1943, rs.getFloat(1), 0);
      Assert.assertEquals((float) 3.1415926, rs.getFloat(2), 0);
      Assert.assertEquals(bigDecimal.floatValue(), rs.getFloat(3), 0);
      Assert.assertEquals((float) 3.1415926, rs.getFloat(4), 0);
    }
    rs.close();
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    // cast from STRING, DECIMAL
    ResultSet rs = stmt.executeQuery(
        String.format("select '%s' c1, %s c2 from dual;", decimalStr, odpsDecimalStr));
    {
      rs.next();
      Assert.assertEquals(bigDecimal, rs.getBigDecimal(1));
      Assert.assertEquals(bigDecimal, rs.getBigDecimal(2));
    }
    rs.close();
  }

  @Test
  public void testGetTimeFormat() throws Exception {
    // cast from STRING, DATETIME
    ResultSet rs = stmt.executeQuery(
        String.format("select '%s' c1, %s c2 from dual;", nowStr, odpsNowStr));
    {
      rs.next();
      Assert.assertEquals(new Date(unixTimeNow).toString(), rs.getDate(1).toString());
      Assert.assertEquals(new Date(unixTimeNow).toString(), rs.getDate(2).toString());

      Assert.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(1).toString());
      Assert.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(2).toString());

      Assert.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(1)));
      Assert.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(2)));
    }
    rs.close();
  }

  @Test
  public void testGetString() throws Exception {
    // cast from STRING, DOUBLE, BIGINT, DATETIME, DECIMAL
    ResultSet rs = stmt.executeQuery(
        String.format("select 'alibaba' c1, 0.5 c2, 1 c3, %s c4, %s c5, true c6 from dual;",
                      odpsNowStr, odpsDecimalStr));
    {
      rs.next();
      Assert.assertEquals("alibaba", rs.getString(1));
      Assert.assertEquals("alibaba", rs.getString("c1"));
      Assert.assertEquals("0.5", rs.getString(2));
      Assert.assertEquals("1", rs.getString(3));
      Assert.assertEquals(nowStr, rs.getString(4));
      Assert.assertEquals(decimalStr, rs.getString(5));
      Assert.assertEquals(Boolean.TRUE.toString(), rs.getString(6));
    }
    rs.close();
  }
}
