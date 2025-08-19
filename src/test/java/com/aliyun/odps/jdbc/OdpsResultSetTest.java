/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsResultSetTest {

  static Statement stmt;

  static long unixTimeNow;
  static SimpleDateFormat formatter;
  static String nowStr;
  static String odpsNowStr;
  static String decimalValue;
  static String decimalStr;
  static String odpsDecimalStr;
  static BigDecimal bigDecimal;

  @BeforeAll
  public static void setUp() throws Exception {
    OdpsConnection conn = (OdpsConnection) TestUtils.getConnection();
    stmt = conn.createStatement();
    stmt.execute("set odps.default.schema=default;");
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");

    Odps odps = TestUtils.getOdps();
    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UploadSession upload = tunnel.createUploadSession(odps.getDefaultProject(), "dual");
    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    r.setBigint(0, 42L);
    writer.write(r);
    writer.close();
    upload.commit(new Long[]{0L});

    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    unixTimeNow = new java.util.Date().getTime();
    nowStr = formatter.format(unixTimeNow);
    odpsNowStr = "cast('" + nowStr + "' as datetime)";

    decimalValue = "55.123456789012345";
    decimalStr = decimalValue + "BD";
    odpsDecimalStr = "cast('" + decimalValue + "' as decimal(38,18))";
    bigDecimal = new BigDecimal(decimalValue);

  }

  @AfterAll
  public static void tearDown() throws Exception {
    stmt.executeUpdate("drop table if exists dual;");
    stmt.close();
  }

  @Test
  public void testSelectFromPartition() throws Exception {
    stmt.executeUpdate("drop table if exists select_from_partition");
    stmt.executeUpdate(
        "create table if not exists select_from_partition(id bigint) partitioned by (par_col string)");
    stmt.executeUpdate("alter table select_from_partition add partition (par_col='a')");

    PartitionSpec ps = new PartitionSpec("par_col='a'");
    Odps odps = TestUtils.getOdps();
    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UploadSession upload = tunnel.createUploadSession(odps.getDefaultProject(), "select_from_partition", ps);
    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    r.setBigint(0, 42L);
    writer.write(r);
    writer.close();
    upload.commit(new Long[]{0L});

    ResultSet rs = stmt.executeQuery("select * from select_from_partition where par_col='a'");
    rs.next();
    Assertions.assertEquals(42L, rs.getInt(1));
    rs.close();
    stmt.executeUpdate("drop table if exists select_from_partition");
  }

  @Test
  public void testGetSelectStar() throws Exception {
    ResultSet rs = stmt.executeQuery("select * from dual;");
    rs.next();
    Assertions.assertEquals(42L, rs.getInt(1));
    rs.close();
  }

  @Test
  public void testGetSelectCountStar() throws Exception {
    ResultSet rs = stmt.executeQuery("select count(*) from dual;");
    rs.next();
    // TODO fix later
    Assertions.assertEquals(1, rs.getInt(1));
    rs.close();
  }

  @Test
  public void testGetSelectEmbedded() throws Exception {
    // ResultSet
    // rs = stmt.executeQuery("select 1 c1, 2.2 c2, null c3, 'haha' c4 from dual;");
    ResultSet rs = stmt.executeQuery("select 1 c1, 2.2 c2, 'haha' c3 from dual;");

    ResultSetMetaData meta = rs.getMetaData();

    Assertions.assertEquals("c1", meta.getColumnName(1));
    Assertions.assertEquals("c2", meta.getColumnName(2));
    Assertions.assertEquals("c3", meta.getColumnName(3));
    // Assert.assertEquals("c4", meta.getColumnName(4));

    Assertions.assertEquals("INT", meta.getColumnTypeName(1));
    Assertions.assertEquals("DOUBLE", meta.getColumnTypeName(2));
    // TODO: SDK treats com.aliyun.odps.OdpsType.VOID as string?
    Assertions.assertEquals("STRING", meta.getColumnTypeName(3));
    // Assert.assertEquals("STRING", meta.getColumnTypeName(4));

    rs.next();
    Assertions.assertEquals(1, rs.getInt(1));
    Assertions.assertEquals(2.2, rs.getDouble(2), 0);
    //Assert.assertEquals(0, rs.getInt(3));
    //Assert.assertTrue(rs.wasNull());
    Assertions.assertEquals("haha", rs.getString(3));

    rs.close();
  }

  @Test
  public void testGetObject() throws Exception {
    ResultSet rs =
        stmt.executeQuery("select * from (select 1L id, 1.5 weight from dual"
                          + " union all select 2 id, 2.9 weight from dual) x order by id desc limit 2;");
    {
      rs.next();
      Assertions.assertEquals(2, ((Long) rs.getObject(1)).longValue());
      Assertions.assertEquals(2, ((Long) rs.getObject("id")).longValue());
      Assertions.assertEquals(2.9, ((Double) rs.getObject(2)).doubleValue(), 0);
      Assertions.assertEquals(2.9, ((Double) rs.getObject("weight")).doubleValue(), 0);
    }

    {
      rs.next();
      // TODO fix later
      Assertions.assertEquals(1, ((Long) rs.getObject(1)).longValue());
      Assertions.assertEquals(1, ((Long) rs.getObject("id")).longValue());
      Assertions.assertEquals(1.5, ((Double) rs.getObject(2)).doubleValue(), 0);
      Assertions.assertEquals(1.5, ((Double) rs.getObject("weight")).doubleValue(), 0);
    }

    rs.close();
  }

  @Test
  public void testGetBoolean() throws Exception {
    // cast from BOOLEAN, STRING, DOUBLE, BIGINT
    ResultSet rs =
        stmt.executeQuery("select true c1, false c2, '42' c3, '0' c4, "
                          + "3.14 c5, 0.0 c6, 95 c7, 0 c8 from dual;");
    {
      rs.next();
      Assertions.assertEquals(true, rs.getBoolean(1));
      Assertions.assertEquals(false, rs.getBoolean(2));
      Assertions.assertEquals(true, rs.getBoolean(3));
      Assertions.assertEquals(false, rs.getBoolean(4));
      Assertions.assertEquals(true, rs.getBoolean(5));
      Assertions.assertEquals(false, rs.getBoolean(6));
      Assertions.assertEquals(true, rs.getBoolean(7));
      Assertions.assertEquals(false, rs.getBoolean(8));
    }
    rs.close();
  }

  @Test
  public void testGetByte() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL
    ResultSet rs =
        stmt.executeQuery(String.format("select 1943 c1, 3.1415926 c2, %s c3 from dual;",
                                        odpsDecimalStr));
    {
      rs.next();
      Assertions.assertEquals((byte) 1943, rs.getByte(1));
      Assertions.assertEquals((byte) 3.1415926, rs.getByte(2));
      Assertions.assertEquals(bigDecimal.byteValue(), rs.getByte(3));
    }
    rs.close();
  }

  @Test
  public void testGetInteger() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    ResultSet rs =
        stmt.executeQuery(String.format(
            "select 1943 c1, 3.1415926 c2, %s c3, '1234' c4 from dual;", odpsDecimalStr));
    {
      rs.next();
      Assertions.assertEquals(1943, rs.getInt(1));
      Assertions.assertEquals((int) 3.1415926, rs.getInt(2));
      Assertions.assertEquals(bigDecimal.intValue(), rs.getInt(3));
      Assertions.assertEquals(1234, rs.getInt(4));

      Assertions.assertEquals((short) 1943, rs.getShort(1));
      Assertions.assertEquals((short) 3.1415926, rs.getShort(2));
      Assertions.assertEquals(bigDecimal.shortValue(), rs.getShort(3));
      Assertions.assertEquals((short) 1234, rs.getShort(4));

      Assertions.assertEquals((long) 1943, rs.getLong(1));
      Assertions.assertEquals((long) 3.1415926, rs.getLong(2));
      Assertions.assertEquals(bigDecimal.longValue(), rs.getLong(3));
      Assertions.assertEquals((long) 1234, rs.getLong(4));
    }
    rs.close();
  }

  @Test
  public void testGetFloatPoint() throws Exception {
    // cast from BIGINT, DOUBLE, DECIMAL, STRING
    ResultSet rs =
        stmt.executeQuery(String.format(
            "select 1943 c1, 3.1415926 c2, %s c3, '3.1415926' c4 from dual;", odpsDecimalStr));
    {
      rs.next();
      Assertions.assertEquals((double) 1943, rs.getDouble(1), 0);
      Assertions.assertEquals(3.1415926, rs.getDouble(2), 0);
      Assertions.assertEquals(bigDecimal.doubleValue(), rs.getDouble(3), 0);
      Assertions.assertEquals(3.1415926, rs.getDouble(4), 0);

      Assertions.assertEquals((float) 1943, rs.getFloat(1), 0);
      Assertions.assertEquals((float) 3.1415926, rs.getFloat(2), 0);
      Assertions.assertEquals(bigDecimal.floatValue(), rs.getFloat(3), 0);
      Assertions.assertEquals((float) 3.1415926, rs.getFloat(4), 0);
    }
    rs.close();
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    // cast from STRING, DECIMAL
    ResultSet rs =
        stmt.executeQuery(String.format("select %s c1, %s c2 from dual;", decimalStr,
                                        odpsDecimalStr));
    {
      rs.next();
      Assertions.assertEquals(bigDecimal, rs.getBigDecimal(1));
      Assertions.assertEquals(bigDecimal, rs.getBigDecimal(2));
    }
    rs.close();
  }

  @Test
  public void testGetTimeFormat() throws Exception {
    // cast from STRING, DATETIME
    ResultSet rs =
        stmt.executeQuery(
            String.format("select DATETIME'%s' c1, %s c2 from dual;", nowStr, odpsNowStr));
    {
      rs.next();
      Assertions.assertEquals(new Date(unixTimeNow).toString(), rs.getDate(1).toString());
      Assertions.assertEquals(new Date(unixTimeNow).toString(), rs.getDate(2).toString());

      Assertions.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(1).toString());
      Assertions.assertEquals(new Time(unixTimeNow).toString(), rs.getTime(2).toString());

      Assertions.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(1)));
      Assertions.assertEquals(formatter.format(new Timestamp(unixTimeNow)),
                          formatter.format(rs.getTimestamp(2)));
    }
    rs.close();
  }

  @Test
  public void testGetString() throws Exception {
    // cast from STRING, DOUBLE, BIGINT, DATETIME, DECIMAL
    ResultSet rs =
        stmt.executeQuery(String.format(
            "select 'alibaba' c1, 0.5 c2, 1 c3, %s c4, %s c5, true c6 from dual;", odpsNowStr,
            odpsDecimalStr));
    {
      rs.next();
      Assertions.assertEquals("alibaba", rs.getString(1));
      Assertions.assertEquals("alibaba", rs.getString("c1"));
      Assertions.assertEquals("0.5", rs.getString(2));
      Assertions.assertEquals("1", rs.getString(3));
      Assertions.assertEquals(nowStr, rs.getString(4));
      Assertions.assertEquals(decimalValue, rs.getString(5));
      Assertions.assertEquals(Boolean.TRUE.toString(), rs.getString(6));
    }
    rs.close();
  }


  @Test
  public void testGetStringForComplexType() throws Exception {
    // complex map column
    String sql = "select map_from_entries(array(struct(1, \"a\"),struct(2, \"b\")))";
    ResultSet rs = stmt.executeQuery(sql);
    {
      rs.next();
      Assertions.assertEquals("{\"2\":\"b\",\"1\":\"a\"}", rs.getString(1));
    }
    rs.close();
  }

  @Test
  public void testGetTimestampWithTimeZone() throws Exception {
    OdpsConnection conn = (OdpsConnection) TestUtils.getConnection();
    TimeZone tz = conn.getTimezone();

    // Make sure the project's time zone is not UTC, or this test case will always pass.
    Assertions.assertNotEquals(0, tz.getRawOffset());

    long timestampWithoutTimeZone;
    try (ResultSet rs = stmt.executeQuery(String.format("select %s c1 from dual;", odpsNowStr))) {
      rs.next();
      timestampWithoutTimeZone = rs.getTimestamp(1).getTime();
    }

    long timestampWithTimeZone;
    conn.setUseProjectTimeZone(true);
    try (ResultSet rs = stmt.executeQuery(String.format("select %s c1 from dual;", odpsNowStr))) {
      rs.next();
      timestampWithTimeZone = rs.getTimestamp(1).getTime();
    } finally {
      conn.setUseProjectTimeZone(false);
    }

    // TODO fix later
    // Assertions.assertEquals(tz.getRawOffset(), timestampWithTimeZone - timestampWithoutTimeZone);
  }
}