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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsResultSetTest {

  static long unixTimeNow;
  static String odpsDatetimeNow;

  @BeforeClass
  public static void setUp() throws Exception {
    unixTimeNow = new java.util.Date().getTime();
    Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    odpsDatetimeNow = formatter.format(unixTimeNow);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testGetObject() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();
    String sql = "select * from" + "(select 1 id, 1.5 weight from dual" + " union all"
                 + " select 2 id, 2.9 weight from dual) x;";
    ResultSet rs = stmt.executeQuery(sql);

    rs.next();
    Assert.assertEquals(1, ((Long) rs.getObject(1)).longValue());
    Assert.assertEquals(1, ((Long) rs.getObject("id")).longValue());
    Assert.assertEquals(1.5, ((Double) rs.getObject(2)).doubleValue(), 0);
    Assert.assertEquals(1.5, ((Double) rs.getObject("weight")).doubleValue(), 0);

    rs.next();
    Assert.assertEquals(2, ((Long) rs.getObject(1)).longValue());
    Assert.assertEquals(2, ((Long) rs.getObject("id")).longValue());
    Assert.assertEquals(2.9, ((Double) rs.getObject(2)).doubleValue(), 0);
    Assert.assertEquals(2.9, ((Double) rs.getObject("weight")).doubleValue(), 0);
    rs.close();
  }

  @Test
  public void testForgetToClose() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt("id"));
//    rs.close();   forget to close by purpose !!!

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals((int) 3.1415926, rs.getInt(1));
//    rs.close();   forget to close by purpose !!!

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    rs.close();
  }

  @Test
  public void testGetBoolean() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BOOLEAN
    ResultSet rs = stmt.executeQuery("select " + Boolean.TRUE + " male from dual;");
    rs.next();
    Assert.assertEquals(true, rs.getBoolean(1));
    Assert.assertEquals(true, rs.getBoolean("male"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '0' namex, '42' namey from dual;");
    rs.next();
    Assert.assertEquals(false, rs.getBoolean(1));
    Assert.assertEquals(true, rs.getBoolean(2));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals(true, rs.getBoolean(1));
    rs.close();

    // cast from BIGINT
    rs = stmt.executeQuery("select 42 idx, 0 idy from dual;");
    rs.next();
    Assert.assertEquals(true, rs.getBoolean(1));
    Assert.assertEquals(false, rs.getBoolean(2));
    rs.close();
  }

  @Test
  public void testGetByte() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((byte) Long.MAX_VALUE, rs.getByte(1));
    Assert.assertEquals((byte) Long.MAX_VALUE, rs.getByte("id"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals((byte) 3.1415926, rs.getByte(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.byteValue(), rs.getByte(1));
    rs.close();
  }

  @Test
  public void testGetInt() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt("id"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals((int) 3.1415926, rs.getInt(1));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals((int) Long.MAX_VALUE, rs.getInt(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.intValue(), rs.getInt(1));
    rs.close();
  }

  @Test
  public void testGetShort() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((short) Long.MAX_VALUE, rs.getShort(1));
    Assert.assertEquals((short) Long.MAX_VALUE, rs.getShort("id"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals((short) 3.1415926, rs.getShort(1));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals((short) Long.MAX_VALUE, rs.getShort(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.shortValue(), rs.getShort(1));
    rs.close();
  }

  @Test
  public void testGetLong() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from BIGINT
    ResultSet rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals(Long.MAX_VALUE, rs.getLong(1));
    Assert.assertEquals(Long.MAX_VALUE, rs.getLong("id"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select " + 3.1415926  + " id from dual;");
    rs.next();
    Assert.assertEquals((long) 3.1415926, rs.getLong(1));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Long.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals(Long.MAX_VALUE, rs.getLong(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.longValue(), rs.getLong(1));
    rs.close();
  }

  @Test
  public void testGetDouble() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DOUBLE
    ResultSet rs = stmt.executeQuery("select " + Double.MAX_VALUE + " weight from dual;");
    rs.next();
    Assert.assertEquals(Double.MAX_VALUE, rs.getDouble(1), 0);
    Assert.assertEquals(Double.MAX_VALUE, rs.getDouble("weight"), 0);
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Double.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals(Double.MAX_VALUE, rs.getDouble(1), 0);
    rs.close();

    // Cast from BIGINT
    rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((double) Long.MAX_VALUE, rs.getDouble(1), 0);
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.doubleValue(), rs.getDouble(1), 0);
    rs.close();
  }

  @Test
  public void testGetFloat() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DOUBLE
    ResultSet rs = stmt.executeQuery("select " + Double.MAX_VALUE + " weight from dual;");
    rs.next();
    Assert.assertEquals((float) Double.MAX_VALUE, rs.getFloat(1), 0);
    Assert.assertEquals((float) Double.MAX_VALUE, rs.getFloat("weight"), 0);
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + Double.MAX_VALUE + "' name from dual;");
    rs.next();
    Assert.assertEquals((float) Double.MAX_VALUE, rs.getFloat(1), 0);
    rs.close();

    // Cast from BIGINT
    rs = stmt.executeQuery("select " + Long.MAX_VALUE + " id from dual;");
    rs.next();
    Assert.assertEquals((float) Long.MAX_VALUE, rs.getFloat(1), 0);
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs = stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect.floatValue(), rs.getFloat(1), 0);
    rs.close();
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    ResultSet rs =
        stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    BigDecimal expect = new BigDecimal(decimalStr);
    Assert.assertEquals(expect, rs.getBigDecimal(1));
    Assert.assertEquals(expect, rs.getBigDecimal("weight"));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + decimalStr + "' name from dual;");
    rs.next();
    Assert.assertEquals(expect, rs.getBigDecimal(1));
    rs.close();
  }

  @Test
  public void testGetDate() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Date expect = new Date(unixTimeNow);
    Assert.assertEquals(expect.toString(), rs.getDate(1).toString());
    Assert.assertEquals(expect.toString(), rs.getDate("day").toString());
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    Assert.assertEquals(expect.toString(), rs.getDate(1).toString());
    rs.close();
  }

  @Test
  public void testGetTime() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Time expect = new Time(unixTimeNow);
    Assert.assertEquals(expect.toString(), rs.getTime(1).toString());
    Assert.assertEquals(expect.toString(), rs.getTime("day").toString());
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    Assert.assertEquals(expect.toString(), rs.getTime(1).toString());
    rs.close();
  }

  @Test
  public void testGetTimeStamp() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from DATETIME
    ResultSet rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");

    rs.next();
    Timestamp expect = new Timestamp(unixTimeNow);
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Assert.assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp(1)));
    Assert.assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp("day")));
    rs.close();

    // cast from STRING
    rs = stmt.executeQuery("select '" + odpsDatetimeNow + "' name from dual;");
    rs.next();
    Assert.assertEquals(formatter.format(expect), formatter.format(rs.getTimestamp(1)));
    rs.close();
  }

  // Do not forget to modify the charset and run this test
  @Test
  public void testGetNonUTFString() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from STRING
    ResultSet rs = stmt.executeQuery("select '你好' name from dual;");
    rs.next();
    System.out.println(rs.getString(1));
    rs.close();

    stmt.close();
  }

  @Test
  public void testGetString() throws Exception {
    Statement stmt = OdpsConnectionFactory.getInstance().conn.createStatement();

    // cast from STRING
    ResultSet rs = stmt.executeQuery("select 'alibaba' name from dual;");
    rs.next();
    Assert.assertEquals("alibaba", rs.getString(1));
    Assert.assertEquals("alibaba", rs.getString("name"));
    rs.close();

    // cast from DOUBLE
    rs = stmt.executeQuery("select 0.5 weight from dual;");
    rs.next();
    Assert.assertEquals("0.5", rs.getString(1));
    rs.close();

    // cast from BIGINT
    rs = stmt.executeQuery("select 1 id from dual;");
    rs.next();
    Assert.assertEquals("1", rs.getString(1));
    rs.close();

    // cast from DATETIME
    rs = stmt.executeQuery(
        "select cast('" + odpsDatetimeNow + "' as datetime) day from dual;");
    rs.next();
    Assert.assertEquals(odpsDatetimeNow, rs.getString(1));
    rs.close();

    // cast from DECIMAL
    String decimalStr = "55.123456789012345";
    rs =
        stmt.executeQuery("select cast('" + decimalStr + "' as decimal) weight from dual;");
    rs.next();
    Assert.assertEquals(decimalStr, rs.getString(1));
    rs.close();

    // cast from BOOLEAN
    rs = stmt.executeQuery("select " + Boolean.TRUE + " male from dual;");
    rs.next();
    Assert.assertEquals(Boolean.TRUE.toString(), rs.getString(1));
    rs.close();
  }
}
