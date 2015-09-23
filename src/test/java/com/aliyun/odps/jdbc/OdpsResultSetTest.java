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
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.junit.Test;
import org.junit.Assert;

import com.aliyun.odps.OdpsType;

public class OdpsResultSetTest {

  static long unixTimeNow = new java.util.Date().getTime();
  static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static String nowStr = formatter.format(unixTimeNow);
  static String decimalStr = "55.123456789012345";

  @Test
  public void testGetObject() throws Exception {
    Object[][] rows = new Object[2][];
    rows[0] = new Object[]{Long.valueOf(1L), Double.valueOf(1.5D)};
    rows[1] = new Object[]{Long.valueOf(2L), Double.valueOf(2.9D)};
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(Arrays.asList("id", "weight"),
                                                       Arrays.asList(OdpsType.BIGINT, OdpsType.DOUBLE));
    MockResultSet rs = new MockResultSet(rows, meta);
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
    // cast from BOOLEAN, DOUBLE, BIGINT, STRING
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Boolean.TRUE, Boolean.FALSE, Long.valueOf(42L), Long.valueOf(0L),
                            Double.valueOf(3.14D), Double.valueOf(0.0D), "95", "0"};
    MockResultSet rs = new MockResultSet(rows, null);
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
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr)};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals((byte) 1943, rs.getByte(1));
      Assert.assertEquals((byte) 3.1415926, rs.getByte(2));
      Assert.assertEquals(new BigDecimal("55.123456789012345").byteValue(), rs.getByte(3));
    }
    rs.close();
  }

  @Test
  public void testGetInt() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr), "1234"};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals(1943, rs.getInt(1));
      Assert.assertEquals((int) 3.1415926, rs.getInt(2));
      Assert.assertEquals(new BigDecimal(decimalStr).intValue(), rs.getInt(3));
      Assert.assertEquals(1234, rs.getInt(4));
    }
    rs.close();
  }

  @Test
  public void testGetShort() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    Object[][] rows = new Object[1][];
    rows[0] =
        new Object[]{Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr),
                     "1234"};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals((short) 1943, rs.getShort(1));
      Assert.assertEquals((short) 3.1415926, rs.getShort(2));
      Assert.assertEquals(new BigDecimal(decimalStr).shortValue(), rs.getShort(3));
      Assert.assertEquals((short) 1234, rs.getShort(4));
      rs.close();
    }
  }

  @Test
  public void testGetLong() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr), "1234"};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals((long) 1943, rs.getLong(1));
      Assert.assertEquals((long) 3.1415926, rs.getLong(2));
      Assert.assertEquals(new BigDecimal(decimalStr).longValue(), rs.getLong(3));
      Assert.assertEquals((long) 1234, rs.getLong(4));
    }
    rs.close();
  }

  @Test
  public void testGetDouble() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr), "5.12345"};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals((double) 1943, rs.getDouble(1), 0);
      Assert.assertEquals(3.1415926D, rs.getDouble(2), 0);
      Assert.assertEquals(new BigDecimal(decimalStr).doubleValue(), rs.getDouble(3), 0);
      Assert.assertEquals(5.12345D, rs.getDouble(4), 0);
    }
    rs.close();
  }

  @Test
  public void testGetFloat() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {Long.valueOf(1943L), Double.valueOf(3.1415926D), new BigDecimal(decimalStr), "5.12345"};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals((float) 1943, rs.getFloat(1), 0);
      Assert.assertEquals(3.1415926F, rs.getFloat(2), 0);
      Assert.assertEquals(new BigDecimal(decimalStr).floatValue(), rs.getFloat(3), 0);
      Assert.assertEquals(5.12345F, rs.getFloat(4), 0);
    }
    rs.close();
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    // cast from STRING, DECIMAL
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {decimalStr, new BigDecimal(decimalStr)};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals(new BigDecimal(decimalStr), rs.getBigDecimal(1));
      Assert.assertEquals(new BigDecimal(decimalStr), rs.getBigDecimal(2));
    }
    rs.close();
  }

  @Test
  public void testGetDate() throws Exception {
    // cast from STRING, DATETIME
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {nowStr, new java.util.Date(unixTimeNow)};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals(new java.sql.Date(unixTimeNow).toString(), rs.getDate(1).toString());
      Assert.assertEquals(new java.sql.Date(unixTimeNow).toString(), rs.getDate(2).toString());
    }
    rs.close();
  }

  @Test
  public void testGetTime() throws Exception {
    // cast from STRING, DATETIME
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {nowStr, new java.util.Date(unixTimeNow)};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(1).toString());
      Assert.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(2).toString());
     }
    rs.close();
  }

  @Test
  public void testGetTimestamp() throws Exception {
    // cast from STRING, DATETIME
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {nowStr, new java.util.Date(unixTimeNow)};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(1)));
      Assert.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(2)));
    }
    rs.close();
  }

  @Test
  public void testGetString() throws Exception {
    // cast from STRING, DOUBLE, BIGINT, DATETIME, DECIMAL, BOOLEAN
    Object[][] rows = new Object[1][];
    rows[0] = new Object[] {"alibaba", Double.valueOf(0.5D), Long.valueOf(1L), new java.util.Date(unixTimeNow),
                            new BigDecimal(decimalStr), Boolean.TRUE};
    MockResultSet rs = new MockResultSet(rows, null);
    {
      rs.next();
      Assert.assertEquals("alibaba", rs.getString(1));
      Assert.assertEquals("0.5", rs.getString(2));
      Assert.assertEquals("1", rs.getString(3));
      Assert.assertEquals(nowStr, rs.getString(4));
      Assert.assertEquals(decimalStr, rs.getString(5));
      Assert.assertEquals("true", rs.getString(6));
    }
    rs.close();
  }
}
