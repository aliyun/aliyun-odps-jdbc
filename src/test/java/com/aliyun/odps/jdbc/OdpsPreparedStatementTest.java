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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsPreparedStatementTest {

  @Test
  public void testSetPartialWithoutPartition() throws SQLException, OdpsException, IOException {
    OdpsConnection connection = (OdpsConnection) TestManager.getInstance().conn;
    Statement stmt = connection.createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(c1 bigint, c2 bigint, c3 bigint);");

    testExecuteBatch("insert into dual(c3, c1, c2) values(? ,?, ?);");
    show("dual");

    testExecuteBatch("insert into dual values(? ,?, ?);");
    show("dual");
  }

  @Test
  public void testSetPartialWithPartition() throws SQLException, OdpsException, IOException {
    OdpsConnection connection = (OdpsConnection) TestManager.getInstance().conn;
    Statement stmt = connection.createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(c1 bigint, c2 bigint, c3 bigint) partitioned by (c4 int);");

    testExecuteBatch("insert into dual(c3, c1, c2) partition(c4=1) values(? ,?, ?);");
    show("dual");

    testExecuteBatch("insert into dual partition(c4=1) values(? ,?, ?);");
    show("dual");
  }

  private void testExecuteBatch(String sql) throws SQLException {
    OdpsConnection connection = (OdpsConnection) TestManager.getInstance().conn;
    PreparedStatement pstmt = connection.prepareStatement(sql);
    pstmt.setLong(1, 100L);
    pstmt.setLong(2, 200L);
    pstmt.setLong(3, 300L);
    pstmt.addBatch();
    pstmt.setLong(1, 200L);
    pstmt.setLong(2, 200L);
    pstmt.setLong(3, 300L);
    pstmt.addBatch();
    pstmt.setLong(1, 300L);
    pstmt.setLong(2, 200L);
    pstmt.setLong(3, 300L);
    pstmt.addBatch();
    pstmt.executeBatch();
    pstmt.close();
  }

  private void show(String tableName) throws IOException, OdpsException {
    OdpsConnection connection = (OdpsConnection) TestManager.getInstance().conn;
    RecordReader reader = connection.getOdps().tables().get(tableName).read(10);
    Record r;
    while((r = reader.read()) != null) {
      for(int i = 0; i < r.getColumnCount(); i++) {
        System.out.print(Optional.ofNullable(r.get(i)).map(Object::toString).orElse("NULL") + " ");
      }
      System.out.println();
    }
  }


  @Test
  public void testSetAll() throws Exception {
    Statement stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");
    TableTunnel.UploadSession upload = TestManager.getInstance().tunnel.createUploadSession(
        TestManager.getInstance().odps.getDefaultProject(), "dual");
    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    r.setBigint(0, 42L);
    writer.write(r);
    writer.close();
    upload.commit(new Long[]{0L});

    PreparedStatement pstmt;
    pstmt = TestManager.getInstance().conn.prepareStatement(
        "select ? c1, ? c2, ? c3, ? c4, ? c5, ? c6, "
        + "? c7, ? c8, ? c9, ? c10, ? c11, ? c12, ? c13 from dual;");
    long unixtime = new java.util.Date().getTime();

    pstmt.setBigDecimal(1, BigDecimal.TEN);
    pstmt.setBoolean(2, Boolean.TRUE);
    pstmt.setByte(3, Byte.MAX_VALUE);
    pstmt.setDate(4, new Date(unixtime));
    pstmt.setDouble(5, Double.MAX_VALUE);
    pstmt.setFloat(6, Float.MAX_VALUE);
    pstmt.setInt(7, Integer.MAX_VALUE);
    pstmt.setObject(8, 0.314);
    pstmt.setLong(9, Long.MAX_VALUE);
    pstmt.setShort(10, Short.MAX_VALUE);
    pstmt.setString(11, "hello");
    pstmt.setTime(12, new Time(unixtime));
    pstmt.setTimestamp(13, Timestamp.valueOf("2019-05-27 00:00:00.123456789"));

    {
      ResultSet rs = pstmt.executeQuery();
      rs.next();
      Assert.assertEquals(BigDecimal.TEN, rs.getBigDecimal(1));
      Assert.assertEquals(Boolean.TRUE, rs.getBoolean(2));
      Assert.assertEquals(Byte.MAX_VALUE, rs.getByte(3));
      Assert.assertEquals(new Date(unixtime).toString(), rs.getDate(4).toString());
      Assert.assertEquals(Double.MAX_VALUE, rs.getDouble(5), 0);
      Assert.assertEquals(Float.MAX_VALUE, rs.getFloat(6), 0);
      Assert.assertEquals(Integer.MAX_VALUE, rs.getInt(7));
      Assert.assertEquals(0.314, rs.getDouble(8), 0);
      Assert.assertEquals(Long.MAX_VALUE, rs.getLong(9));
      Assert.assertEquals(Short.MAX_VALUE, rs.getShort(10));
      Assert.assertEquals("hello", rs.getString(11));
      Assert.assertEquals(new Time(unixtime).toString(), rs.getTime(12).toString());
      Assert.assertEquals("2019-05-27 00:00:00.123456789",
                          rs.getTimestamp(13).toString());
      rs.close();
    }
    pstmt.close();
  }

  @Test
  public void testSetAllWithNewType() throws SQLException, ParseException {
    Connection conn = TestManager.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.execute("set odps.sql.type.system.odps2=true;");
    ddl.execute("set odps.sql.decimal.odps2=true;");
    ddl.executeUpdate("drop table if exists insert_with_new_type;");
    ddl.executeUpdate("create table insert_with_new_type(c1 TINYINT, c2 SMALLINT, c3 INT,"
                      + "c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38, 18), c8 VARCHAR(255),"
                      + "c9 STRING, c10 DATETIME, c11 TIMESTAMP, c12 BOOLEAN, c13 DATE, c14 BINARY,"
                      + "c15 DATETIME, c16 TIMESTAMP);");
    PreparedStatement ps = conn.prepareStatement("insert into insert_with_new_type values "
                                                 + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    java.util.Date datetime = datetimeFormat.parse("2019-09-23 14:25:00");
    java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2019-09-23 14:33:57.777");
    Calendar gmtCalendar = new Calendar
        .Builder()
        .setTimeZone(TimeZone.getTimeZone("GMT"))
        .setCalendarType("iso8601")
        .set(Calendar.YEAR, 2020)
        .set(Calendar.MONTH, Calendar.JANUARY)
        .set(Calendar.DAY_OF_MONTH, 1).build();
    java.sql.Date date = new java.sql.Date(gmtCalendar.getTime().getTime());
    ZonedDateTime
        zonedDateTime =
        ZonedDateTime.of(2023, 3, 16, 14, 44, 2, 0, ZoneId.systemDefault());

    ps.setByte(1, new Byte("127"));
    ps.setShort(2, new Short("32767"));
    ps.setInt(3, new Integer("2147483647"));
    ps.setLong(4, new Long("9223372036854775807"));
    ps.setFloat(5, new Float("3.14"));
    ps.setDouble(6, new Double("3.141592653589"));
    ps.setBigDecimal(7, new BigDecimal("3.1415926535897932"));
    ps.setString(8, "foo");
    ps.setString(9, "bar");
    // ps.setDate只能给日期赋值，to ODPS DATE
    ps.setTime(10, new java.sql.Time(datetime.getTime()));
    ps.setTimestamp(11, timestamp);
    ps.setBoolean(12, true);
    ps.setDate(13, date);
    ps.setObject(14, new Binary("FA34E10293CB42848573A4E39937F479".getBytes()));
    ps.setObject(15, zonedDateTime);
    ps.setObject(16, zonedDateTime.toInstant());

    ps.executeUpdate();

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from insert_with_new_type;");
    while (rs.next()) {
      Assert.assertEquals(127, (byte) rs.getObject(1));
      Assert.assertEquals(32767, (short) rs.getObject(2));
      Assert.assertEquals(2147483647, (int) rs.getObject(3));
      Assert.assertEquals(9223372036854775807L, (long) rs.getObject(4));
      Assert.assertEquals(3.14, (float) rs.getObject(5), 0.001);
      Assert.assertEquals(3.141592653589,
                          (double) rs.getObject(6), 0.0000000000001);
      Assert.assertEquals(new BigDecimal("3.1415926535897932"),
                          rs.getObject(7));
      Assert.assertEquals(new Varchar("foo"), rs.getObject(8));
      Assert.assertEquals("bar", rs.getObject(9));
      Assert.assertEquals(new java.sql.Time(datetime.getTime()).toString(),
                          rs.getTime(10).toString());
      Assert.assertEquals(timestamp.toString(), rs.getTimestamp(11).toString());
      Assert.assertEquals(true, rs.getObject(12));
      Assert.assertEquals(date.toString(), rs.getDate(13).toString());
      Assert.assertEquals("=FA4=E1=02=93=CBB=84=85s=A4=E3=997=F4y", rs.getObject(14).toString());
      Assert.assertEquals(zonedDateTime.toString(), rs.getObject(15).toString());
      Assert.assertEquals(zonedDateTime.toInstant().toEpochMilli(),
                          ((Instant) rs.getObject(16)).toEpochMilli());
    }

    ddl.executeUpdate("drop table if exists batch_insert_with_new_type;");
    ddl.close();
  }

  @Test
  public void testBatchInsert() throws Exception {
    Connection conn = TestManager.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists employee_test;");
    ddl.executeUpdate(
        "create table employee_test(c1 bigint, c2 string, c3 datetime, c4 boolean, c5 double, c6 decimal);");
    ddl.close();

    PreparedStatement ps = conn.prepareStatement(
        "insert into employee_test values (?, ?, ?, ?, ?, ?);");

    final int batchSize = 10000;
    int count = 0;

    long unixtime = new java.util.Date().getTime();

    for (int i = 0; i < 10; i++) {
      ps.setLong(1, 9999);
      ps.setString(2, "hello");
      ps.setTime(3, new Time(unixtime));
      ps.setBoolean(4, true);
      ps.setDouble(5, 3.141590261234F);
      ps.setBigDecimal(6, BigDecimal.TEN);
      ps.addBatch();
      if (++count % batchSize == 0) {
        ps.executeBatch();
      }
    }
    ps.executeBatch(); // insert remaining records
    ps.close();

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from employee_test");

    while (rs.next()) {
      Assert.assertEquals(rs.getInt(1), 9999);
      Assert.assertEquals(rs.getString(2), "hello");
      Assert.assertEquals(rs.getTime(3).toString(), new Time(unixtime).toString());
      Assert.assertTrue(rs.getBoolean(4));
      Assert.assertEquals(rs.getDouble(5), 3.141590261234F, 0);
      Assert.assertEquals(rs.getBigDecimal(6), BigDecimal.TEN);
      count--;
    }

    Assert.assertEquals(count, 0);

    rs.close();
    query.close();
  }

  @Test
  public void testBatchInsertWithNewType() throws SQLException, ParseException {
    Connection conn = TestManager.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.execute("set odps.sql.type.system.odps2=true;");
    ddl.execute("set odps.sql.decimal.odps2=true;");
    ddl.executeUpdate("drop table if exists batch_insert_with_new_type;");
    ddl.executeUpdate("create table batch_insert_with_new_type(c1 TINYINT, c2 SMALLINT, c3 INT, "
                      + "c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38, 18), c8 VARCHAR(255), "
                      + "c9 STRING, c10 DATETIME, c11 TIMESTAMP, c12 BOOLEAN, c13 DATE);");

    PreparedStatement ps = conn.prepareStatement("insert into batch_insert_with_new_type values "
                                                 + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    // insert 10 rows
    SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    java.util.Date datetime = datetimeFormat.parse("2019-09-23 14:25:00");
    java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2019-09-23 14:33:57.777");
    java.sql.Date date = new java.sql.Date(datetime.getTime());

    for (int i = 0; i < 10; i++) {
      ps.setByte(1, new Byte("127"));
      ps.setShort(2, new Short("32767"));
      ps.setInt(3, new Integer("2147483647"));
      ps.setLong(4, new Long("9223372036854775807"));
      ps.setFloat(5, new Float("3.14"));
      ps.setDouble(6, new Double("3.141592653589"));
      ps.setBigDecimal(7, new BigDecimal("3.141592653589793238"));
      ps.setString(8, "foo");
      ps.setString(9, "bar");
      ps.setTimestamp(10, new java.sql.Timestamp(datetime.getTime()));
      ps.setTimestamp(11, timestamp);
      ps.setBoolean(12, true);
      ps.setDate(13, date);
      ps.addBatch();
    }

    int[] results = ps.executeBatch();
    ps.close();

    for (int i : results) {
      Assert.assertEquals(1, i);
    }

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from batch_insert_with_new_type;");
    while (rs.next()) {
      Assert.assertEquals(127, (byte) rs.getObject(1));
      Assert.assertEquals(32767, (short) rs.getObject(2));
      Assert.assertEquals(2147483647, (int) rs.getObject(3));
      Assert.assertEquals(9223372036854775807L, (long) rs.getObject(4));
      Assert.assertEquals(3.14, (float) rs.getObject(5), 0.001);
      Assert.assertEquals(3.141592653589,
                          (double) rs.getObject(6), 0.0000000000001);
      Assert.assertEquals(new BigDecimal("3.141592653589793238"),
                          rs.getObject(7));
      Assert.assertEquals(new Varchar("foo"), rs.getObject(8));
      Assert.assertEquals("bar", rs.getObject(9));
      Assert.assertEquals("2019-09-23 14:25:00", rs.getString(10));
      Assert.assertEquals(timestamp.toString(), rs.getString(11));
      Assert.assertEquals(true, rs.getObject(12));
      Assert.assertEquals(date.toString(), rs.getDate(13).toString());
    }

    ddl.executeUpdate("drop table if exists batch_insert_with_new_type;");
    ddl.close();
  }

  @Test
  public void test() throws SQLException, ParseException {
    Connection conn = TestManager.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.execute("set odps.sql.type.system.odps2=true;");
    ddl.execute("set odps.sql.decimal.odps2=true;");
    ddl.execute("set odps.sql.type.json.enable=true;");
    ddl.executeUpdate("drop table if exists json_test;");
    ddl.executeUpdate("create table json_test(c1 JSON);");

    PreparedStatement ps = conn.prepareStatement("insert into json_test values "
                                                 + "(?);");

    ps.setString(1, "123");
    ps.addBatch();
    ps.setObject(1, "{\"id\":123,\"name\":\"MaxCompute\"}");
    ps.addBatch();

    int[] results = ps.executeBatch();
    ps.close();

    for (int i : results) {
      Assert.assertEquals(1, i);
    }

    Statement query = conn.createStatement();
    ResultSet
        rs =
        query.executeQuery("set odps.sql.type.json.enable=true; select * from json_test;");
    rs.next();
    Assert.assertEquals(rs.getMetaData().getColumnType(1), 12);
    Assert.assertEquals(((SimpleJsonValue) rs.getObject(1)).getAsNumber().intValue(), 123);
    rs.next();
    Assert.assertEquals(rs.getMetaData().getColumnType(1), 12);
    SimpleJsonValue jsonValue = (SimpleJsonValue) rs.getObject(1);
    Assert.assertEquals(jsonValue.get("id").getAsNumber().intValue(), 123);
    Assert.assertEquals(jsonValue.get("name").getAsString(), "MaxCompute");

    ddl.executeUpdate("drop table if exists json_test;");
    ddl.close();
  }


  @Test
  public void testBatchInsertNullAndFetch() throws Exception {
    Connection conn = TestManager.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists employee_test;");
    ddl.executeUpdate(
        "create table employee_test(c1 bigint, c2 string, c3 datetime, c4 boolean, c5 double, c6 decimal);");
    ddl.close();

    PreparedStatement ps = conn.prepareStatement(
        "insert into employee_test values (?, ?, ?, ?, ?, ?);");

    final int batchSize = 20;
    int count = 0;

    for (int i = 0; i < 120; i++) {
      ps.setNull(1, -1);
      ps.setNull(2, -1);
      ps.setNull(3, -1);
      ps.setNull(4, -1);
      ps.setNull(5, -1);
      ps.setNull(6, -1);

      ps.addBatch();
      if (++count % batchSize == 0) {
        ps.executeBatch();
      }
    }
    ps.executeBatch(); // insert remaining records
    ps.close();

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from employee_test");

    while (rs.next()) {
      Assert.assertEquals(0, rs.getInt(1));
      Assert.assertTrue(rs.wasNull());

      Assert.assertNull(rs.getString(2));
      Assert.assertTrue(rs.wasNull());

      Assert.assertNull(rs.getTime(3));
      Assert.assertTrue(rs.wasNull());

      Assert.assertFalse(rs.getBoolean(4));
      Assert.assertTrue(rs.wasNull());

      Assert.assertEquals(0.0f, rs.getFloat(5), 0);
      Assert.assertTrue(rs.wasNull());

      Assert.assertNull(rs.getBigDecimal(6));
      Assert.assertTrue(rs.wasNull());

      count--;
    }

    Assert.assertEquals(0, count);

    rs.close();
    query.close();
  }

  @Test
  public void testUploadTableWithPartition() throws SQLException {
    Connection conn = TestManager.getInstance().conn;
    String tableName = "shuzuo_prepared_upload_partition_table";

    Statement stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists " + tableName + ";");
    stmt.executeUpdate(
        "create table if not exists " + tableName + " (key1 STRING, key2 DOUBLE, key3 BOOLEAN)"
        + " partitioned by (p1 STRING, p2 STRING);");

    PreparedStatement
        ps =
        conn.prepareStatement(
            "insert into " + tableName + " partition( p1='1234', p2= 2345 )"
            + " values (?, ?, ?);");

    ps.setString(1, "value1");
    ps.setDouble(2, new Double("3.141592653589"));
    ps.setBoolean(3, true);

    ps.executeUpdate();

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from " + tableName + " where p1='1234';");
    while (rs.next()) {
      Assert.assertEquals("value1", rs.getString(1));
      Assert.assertEquals(3.141592653589,
                          (double) rs.getObject(2), 0.0000000000001);
      Assert.assertTrue(rs.getBoolean(3));
    }

    stmt.executeUpdate("drop table if exists " + tableName + ";");

  }


  @Test
  public void testBatchUploadTableWithPartition() throws SQLException {
    Connection conn = TestManager.getInstance().conn;
    String tableName = "shuzuo_prepared_batch_upload_partition_table";

    Statement stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists " + tableName + ";");
    stmt.executeUpdate(
        "create table if not exists " + tableName + " (key1 STRING, key2 DOUBLE, key3 BOOLEAN)"
        + "partitioned by (p1 STRING);");

    PreparedStatement
        ps =
        conn.prepareStatement(
            "insert into " + tableName + " partition(p1=1234)" + " values (?, ?, ?);");

    for (int i = 0; i < 10; i++) {
      ps.setString(1, "value1");
      ps.setDouble(2, new Double("3.141592653589"));
      ps.setBoolean(3, true);
      ps.addBatch();
    }

    ps.executeBatch();
    ps.close();

    Statement query = conn.createStatement();
    ResultSet rs = query.executeQuery("select * from " + tableName + " where p1='1234';");
    while (rs.next()) {
      Assert.assertEquals("value1", rs.getString(1));
      Assert.assertEquals(3.141592653589,
                          (double) rs.getObject(2), 0.0000000000001);
      Assert.assertTrue(rs.getBoolean(3));
    }

    stmt.executeUpdate("drop table if exists " + tableName + ";");

  }

  @Test
  public void testSqlInjection() throws SQLException {
    Connection connection = TestManager.getInstance().conn;
    Statement ddl = connection.createStatement();

    ddl.executeUpdate("drop table if exists sql_injection;");
    ddl.executeUpdate(
        "create table sql_injection(c1 int, c2 string, c3 boolean);");
    ddl.close();

    PreparedStatement ps = connection.prepareStatement(
        "insert into sql_injection values (?, ?, ?);");
    ps.setInt(1, 10);
    ps.setString(2, "test");
    ps.setBoolean(3, true);
    ps.execute();

    ps = connection.prepareStatement("select * from sql_injection where c3 = ?;");
    ps.setObject(1, false);
    ResultSet resultSet = ps.executeQuery();
    Assert.assertFalse(resultSet.next());

    ps.setObject(1, "false', 'or 1=1");
    try {
      ps.execute();
      Assert.fail();
    } catch (Exception ignored) {
    }

  }

  @Test
  public void testSqlWithConstantMark() throws SQLException {
    Connection connection = TestManager.getInstance().conn;
    Statement ddl = connection.createStatement();

    ddl.executeUpdate("drop table if exists sql_with_constant_mark;");
    ddl.executeUpdate(
        "create table sql_with_constant_mark(c1 string, c2 string, c3 string);");
    ddl.close();

    PreparedStatement ps = connection.prepareStatement(
        "insert into sql_with_constant_mark values ('?', ?, ?); --我是后面注释里的?");
    ps.setString(1, "??");
    ps.setString(2, "test");
    ps.execute();

    ps = connection.prepareStatement("--我是前面注释的? \n select c3 from sql_with_constant_mark where c1 = ? AND c2 = \"??\";");
    ps.setObject(1, "?");
    ResultSet resultSet = ps.executeQuery();
    while (resultSet.next()) {
      Assert.assertEquals("test", resultSet.getString(1));
    }
  }

  @Test
  public void testAcid2Table() throws SQLException {
    Connection connection = TestManager.getInstance().conn;
    Statement ddl = connection.createStatement();
    ddl.executeUpdate("drop table if exists acid_table;");
    ddl.executeUpdate(
        "create table acid_table(c1 string not null primary key) tblproperties (\"transactional\"=\"true\");");
    ddl.close();

    PreparedStatement ps = connection.prepareStatement(
        "insert into acid_table values (?);");
    ps.setString(1, "str");
    ps.addBatch();
    ps.executeBatch();

    ps.setString(1, "str2");
    ps.addBatch();
    ps.executeBatch();

    ps.close();

    ResultSet resultSet = runQuery("select count(*) from acid_table;");
    resultSet.next();
    Assert.assertEquals(2, resultSet.getInt(1));

  }

  @Test
  public void testAcidTable() throws SQLException {
    Connection connection = TestManager.getInstance().conn;
    Statement ddl = connection.createStatement();
    ddl.executeUpdate("drop table if exists acid1_table;");
    ddl.executeUpdate(
        "create table acid1_table(c1 string) tblproperties (\"transactional\"=\"true\");");
    ddl.close();
    PreparedStatement ps = connection.prepareStatement("insert into acid1_table values (?);");
    ps.setString(1, "str");
    ps.addBatch();
    ps.executeBatch();
    ps.setString(1, "str2");
    ps.addBatch();
    ps.executeBatch();
    ps.close();
    ResultSet resultSet = runQuery("select count(*) from acid1_table;");
    resultSet.next();
    Assert.assertEquals(2, resultSet.getInt(1));
  }

  ResultSet runQuery(String sql) throws SQLException {
    Statement statement = TestManager.getInstance().conn.createStatement();
    return statement.executeQuery(sql);
  }
}