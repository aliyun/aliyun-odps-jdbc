package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MaxQATest {

  private static Connection conn;
  private static Connection maxqaConn;
  private static Connection maxqaFallbackConn;
  private static Odps odps;
  private static TableTunnel tunnel;
  private static final int ROWS = 10000;

  private static String INPUT_TABLE_NAME = "maxqa_test_table_input";
  private static String OUTPUT_TABLE_NAME = "maxqa_test_table_output";

  @BeforeAll
  public static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    maxqaConn = TestUtils.getConnection(ImmutableMap.of("interactiveMode", "true", "quotaName", "maxqa_huigui_quota"));
    maxqaFallbackConn = TestUtils.getConnection(
        ImmutableMap.of("interactiveMode", "true", "maxqaFallbackEnabled", "true",
                       "maxqaFallbackQuota", "default"));
    odps = TestUtils.getOdps();
    tunnel = new TableTunnel(odps);

    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.executeUpdate("create table if not exists " + INPUT_TABLE_NAME + "(id bigint, name string, value double);");
    stmt.executeUpdate("create table if not exists " + OUTPUT_TABLE_NAME + "(id bigint, name string, value double);");
    stmt.close();

    TableTunnel.UploadSession upload = tunnel.createUploadSession(
        odps.getDefaultProject(), INPUT_TABLE_NAME);

    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    for (int i = 0; i < ROWS; i++) {
      r.setBigint(0, (long) i);
      r.setString(1, "name_" + i);
      r.setDouble(2, i * 1.5);
      writer.write(r);
    }
    writer.close();
    upload.commit(new Long[]{0L});
  }

  @AfterAll
  public static void tearDown() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + INPUT_TABLE_NAME);
    stmt.executeUpdate("drop table if exists " + OUTPUT_TABLE_NAME);
    stmt.close();
  }

  @Test
  public void testBasicQueryWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME + " limit 100");

    int count = 0;
    while (rs.next()) {
      Assertions.assertEquals(count, rs.getInt(1));
      Assertions.assertEquals("name_" + count, rs.getString(2));
      Assertions.assertEquals(count * 1.5, rs.getDouble(3), 0.001);
      count++;
    }
    Assertions.assertEquals(100, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testPreparedStatementWithMaxQA() throws Exception {
    PreparedStatement ps = maxqaConn.prepareStatement(
        "select * from " + INPUT_TABLE_NAME + " where id = ?");
    ps.setInt(1, 100);

    ResultSet rs = ps.executeQuery();
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(100, rs.getInt(1));
    Assertions.assertEquals("name_100", rs.getString(2));
    Assertions.assertEquals(100 * 1.5, rs.getDouble(3), 0.001);
    Assertions.assertFalse(rs.next());

    rs.close();
    ps.close();
  }

  @Test
  public void testBatchInsertWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    stmt.executeUpdate("delete from " + OUTPUT_TABLE_NAME);
    stmt.close();

    PreparedStatement ps = maxqaConn.prepareStatement(
        "insert into " + OUTPUT_TABLE_NAME + " values (?, ?, ?)");

    int batchSize = 10;
    for (int i = 0; i < batchSize; i++) {
      ps.setInt(1, i);
      ps.setString(2, "maxqa_" + i);
      ps.setDouble(3, i * 2.0);
      ps.addBatch();
    }

    int[] results = ps.executeBatch();
    Assertions.assertEquals(batchSize, results.length);
    for (int result : results) {
      Assertions.assertEquals(1, result);
    }
    ps.close();

    Statement queryStmt = maxqaConn.createStatement();
    ResultSet rs = queryStmt.executeQuery("select count(*) from " + OUTPUT_TABLE_NAME);
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(batchSize, rs.getInt(1));

    rs.close();
    queryStmt.close();
  }

  @Test
  public void testSetStatementWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    Assertions.assertFalse(stmt.execute("set odps.sql.select.output.format=csv;"));
    Assertions.assertFalse(stmt.execute("set odps.sql.decimal.odps2=true;"));

    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME + " limit 10");
    int count = 0;
    while (rs.next()) {
      count++;
    }
    Assertions.assertEquals(10, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testComplexQueryWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    String complexSql = "select id, name, sum(value) as total_value from " + INPUT_TABLE_NAME
        + " where id > 100 group by id, name order by id limit 50";

    ResultSet rs = stmt.executeQuery(complexSql);

    int count = 0;
    while (rs.next()) {
      int id = rs.getInt(1);
      String name = rs.getString(2);
      double totalValue = rs.getDouble(3);

      Assertions.assertEquals(101 + count, id);
      Assertions.assertEquals("name_" + (101 + count), name);
      Assertions.assertEquals((101 + count) * 1.5, totalValue, 0.001);
      count++;
    }
    Assertions.assertEquals(50, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testEmptyResultWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select * from " + INPUT_TABLE_NAME + " where id < 0");

    Assertions.assertNotNull(rs);
    Assertions.assertFalse(rs.next());

    rs.close();
    stmt.close();
  }

  @Test
  public void testAggregationWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select count(*), sum(id), avg(value), max(id), min(id) from " + INPUT_TABLE_NAME);

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(ROWS, rs.getLong(1));
    Assertions.assertEquals(ROWS * (ROWS - 1) / 2, rs.getLong(2));
    Assertions.assertEquals((ROWS - 1) * 1.5 / 2, rs.getDouble(3), 0.001);
    Assertions.assertEquals(ROWS - 1, rs.getInt(4));
    Assertions.assertEquals(0, rs.getInt(5));
    Assertions.assertFalse(rs.next());

    rs.close();
    stmt.close();
  }

  @Test
  public void testPreparedStatementWithMultipleParameters() throws Exception {
    PreparedStatement ps = maxqaConn.prepareStatement(
        "select * from " + INPUT_TABLE_NAME + " where id between ? and ? and value > ?");

    ps.setInt(1, 50);
    ps.setInt(2, 100);
    ps.setDouble(3, 75.0);

    ResultSet rs = ps.executeQuery();
    int count = 0;
    while (rs.next()) {
      int id = rs.getInt(1);
      Assertions.assertTrue(id >= 50 && id <= 100);
      Assertions.assertTrue(rs.getDouble(3) > 75.0);
      count++;
    }
    Assertions.assertTrue(count > 0);

    rs.close();
    ps.close();
  }

  @Test
  public void testMaxQAWithFallback() throws Exception {
    Statement stmt = maxqaFallbackConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + INPUT_TABLE_NAME + " limit 50");

    int count = 0;
    while (rs.next()) {
      Assertions.assertEquals(count, rs.getInt(1));
      Assertions.assertEquals("name_" + count, rs.getString(2));
      count++;
    }
    Assertions.assertEquals(50, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testNullHandlingWithMaxQA() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists maxqa_null_test");
    stmt.executeUpdate(
        "create table maxqa_null_test(id bigint, name string, value double)");
    stmt.executeUpdate(
        "insert into maxqa_null_test values (1, null, null), (2, 'test', 3.14), (3, null, 2.5)");
    stmt.close();

    Statement maxqaStmt = maxqaConn.createStatement();
    ResultSet rs = maxqaStmt.executeQuery("select * from maxqa_null_test order by id");

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(1, rs.getInt(1));
    Assertions.assertNull(rs.getString(2));
    Assertions.assertTrue(rs.wasNull());

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(2, rs.getInt(1));
    Assertions.assertEquals("test", rs.getString(2));
    Assertions.assertEquals(3.14, rs.getDouble(3), 0.001);

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(3, rs.getInt(1));
    Assertions.assertNull(rs.getString(2));
    Assertions.assertTrue(rs.wasNull());

    Assertions.assertFalse(rs.next());

    rs.close();
    maxqaStmt.close();

    Statement cleanupStmt = conn.createStatement();
    cleanupStmt.executeUpdate("drop table if exists maxqa_null_test");
    cleanupStmt.close();
  }

  @Test
  public void testStringFunctionsWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select id, upper(name), lower(name), length(name) from " + INPUT_TABLE_NAME
        + " where id < 10 order by id");

    int count = 0;
    while (rs.next()) {
      int id = rs.getInt(1);
      String originalName = "name_" + id;
      Assertions.assertEquals(originalName.toUpperCase(), rs.getString(2));
      Assertions.assertEquals(originalName.toLowerCase(), rs.getString(3));
      Assertions.assertEquals(originalName.length(), rs.getInt(4));
      count++;
    }
    Assertions.assertEquals(10, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testDateFunctionsWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery("select current_timestamp(), current_date()");

    Assertions.assertTrue(rs.next());
    Assertions.assertNotNull(rs.getTimestamp(1));
    Assertions.assertNotNull(rs.getDate(2));
    Assertions.assertFalse(rs.next());

    rs.close();
    stmt.close();
  }

  @Test
  public void testSubqueryWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select * from (select id, name from " + INPUT_TABLE_NAME
        + " where id < 50) t where t.id > 20 order by t.id");

    int count = 0;
    while (rs.next()) {
      int id = rs.getInt(1);
      Assertions.assertTrue(id > 20 && id < 50);
      Assertions.assertEquals("name_" + id, rs.getString(2));
      count++;
    }
    Assertions.assertEquals(29, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testInClauseWithMaxQA() throws Exception {
    PreparedStatement ps = maxqaConn.prepareStatement(
        "select * from " + INPUT_TABLE_NAME + " where id in (?, ?, ?, ?) order by id");

    ps.setInt(1, 10);
    ps.setInt(2, 50);
    ps.setInt(3, 100);
    ps.setInt(4, 500);

    ResultSet rs = ps.executeQuery();
    int[] expectedIds = {10, 50, 100, 500};
    int count = 0;
    while (rs.next()) {
      Assertions.assertEquals(expectedIds[count], rs.getInt(1));
      count++;
    }
    Assertions.assertEquals(4, count);

    rs.close();
    ps.close();
  }

  @Test
  public void testMultipleStatementsWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();

    ResultSet rs1 = stmt.executeQuery("select count(*) from " + INPUT_TABLE_NAME);
    Assertions.assertTrue(rs1.next());
    int count1 = rs1.getInt(1);
    Assertions.assertEquals(ROWS, count1);
    rs1.close();

    ResultSet rs2 = stmt.executeQuery("select sum(id) from " + INPUT_TABLE_NAME);
    Assertions.assertTrue(rs2.next());
    long sum2 = rs2.getLong(1);
    Assertions.assertEquals(ROWS * (ROWS - 1) / 2, sum2);
    rs2.close();

    stmt.close();
  }

  @Test
  public void testOrderByLimitWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select id, name from " + INPUT_TABLE_NAME + " order by id desc limit 20");

    int count = 0;
    while (rs.next()) {
      int expectedId = ROWS - 1 - count;
      Assertions.assertEquals(expectedId, rs.getInt(1));
      Assertions.assertEquals("name_" + expectedId, rs.getString(2));
      count++;
    }
    Assertions.assertEquals(20, count);

    rs.close();
    stmt.close();
  }

  @Test
  public void testCaseWhenWithMaxQA() throws Exception {
    Statement stmt = maxqaConn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "select id, case when id < 5000 then 'small' when id < 8000 then 'medium' else 'large' end as category "
        + "from " + INPUT_TABLE_NAME + " where id in (100, 6000, 9000) order by id");

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(100, rs.getInt(1));
    Assertions.assertEquals("small", rs.getString(2));

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(6000, rs.getInt(1));
    Assertions.assertEquals("medium", rs.getString(2));

    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(9000, rs.getInt(1));
    Assertions.assertEquals("large", rs.getString(2));

    Assertions.assertFalse(rs.next());

    rs.close();
    stmt.close();
  }
}
