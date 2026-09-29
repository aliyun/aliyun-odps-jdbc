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
 */

package com.aliyun.odps.jdbc;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;

/**
 * Contract regressions for binding parameters on {@link PreparedStatement}: NULL shapes, values
 * that carry quotes / backslashes / question marks / multi-byte text, decimal precision, byte[] and
 * LOB-style (stream / Reader) binding, plus the statement-lifecycle half of the contract:
 * {@code clearParameters()}, out-of-range indexes, repeated execution, reuse after {@code close()}
 * and the batch / transaction support that the driver promises today.
 *
 * <p>Every case states the behaviour the driver actually offers. Where that behaviour was a defect
 * the assertion is the fixed one, and the comment records what master used to do (measured on
 * origin/master@3311761 against the real service, see the work item record).
 */
public class OdpsPreparedStatementParamContractTest {

  private static final String TABLE = "jdbc_ps_param_contract";

  /** A backslash without relying on how the source file itself is escaped. */
  private static final String BS = String.valueOf((char) 92);
  private static final String TAB = String.valueOf((char) 9);

  private static Connection conn;
  /** The documented escape hatch of the SQL injection heuristic (values are then interpolated). */
  private static Connection literalConn;

  @BeforeAll
  static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    Map<String, String> props = new HashMap<String, String>();
    props.put("skipSqlInjectCheck", "true");
    literalConn = TestUtils.getConnection(props);

    // odps2 types and the 38,18 decimal are preconditions of this table: declared on every
    // connection this class writes with, not inherited from a project default.
    for (Connection c : new Connection[] {conn, literalConn}) {
      try (Statement flags = c.createStatement()) {
        flags.execute("set odps.sql.type.system.odps2=true;");
        flags.execute("set odps.sql.decimal.odps2=true;");
      }
    }
    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists " + TABLE + ";");
    ddl.executeUpdate("create table " + TABLE + " (id bigint, s string, vc varchar(50), "
                      + "dec38 decimal(38,18), bin binary, i int);");
    ddl.close();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (conn == null) {
      return;
    }
    try (Statement ddl = conn.createStatement()) {
      ddl.executeUpdate("drop table if exists " + TABLE + ";");
    } finally {
      conn.close();
      literalConn.close();
    }
  }

  private static String insertAll() {
    return "insert into " + TABLE + " values (?, ?, ?, ?, ?, ?);";
  }

  private static long countRows(String predicate) throws Exception {
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select count(*) c from " + TABLE + " where " + predicate
                                       + ";")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  // ---------- A1: value shapes ----------

  @Test
  public void nullReachesTheServerThroughEveryNullShapedSetter() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(insertAll())) {
      ps.setNull(1, Types.BIGINT);
      ps.setNull(2, Types.VARCHAR, "string");
      ps.setString(3, null);
      ps.setObject(4, null);
      ps.setArray(5, null);
      ps.setObject(6, null, Types.INTEGER);
      ps.executeUpdate();
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from " + TABLE + " where s is null;")) {
      Assertions.assertTrue(rs.next(), "the all-NULL row was not written");
      for (int i = 1; i <= 6; i++) {
        Assertions.assertNull(rs.getObject(i), "column " + i + " should be NULL");
        Assertions.assertTrue(rs.wasNull(), "wasNull() is false for column " + i);
      }
      Assertions.assertFalse(rs.next(), "exactly one all-NULL row expected");
    }

    // ... and through the constant-SQL path, where NULL must be substituted as the NULL keyword.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1, ? c2 from (select 1 x) t;")) {
      ps.setNull(1, Types.VARCHAR);
      ps.setString(2, "kept");
      try (ResultSet rs = ps.executeQuery()) {
        Assertions.assertTrue(rs.next());
        Assertions.assertNull(rs.getObject(1));
        Assertions.assertTrue(rs.wasNull());
        Assertions.assertEquals("kept", rs.getString(2));
      }
    }
  }

  @Test
  public void specialCharacterValuesRoundTripThroughTheTunnel() throws Exception {
    String quote = "it's";
    String windowsPath = "C:" + BS + "new_folder" + BS + "path";
    String marks = "a?b?c";
    String chinese = "中文测试-测试";
    String keyword = "1 or 1=1";
    String commentMark = "x--y";
    String whitespace = "tab" + TAB + "and" + System.lineSeparator() + "newline";

    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, s) values (?, ?);")) {
      String[] values = {quote, windowsPath, marks, chinese, keyword, commentMark, whitespace};
      for (int i = 0; i < values.length; i++) {
        ps.clearParameters();
        ps.setLong(1, 100L + i);
        ps.setString(2, values[i]);
        ps.addBatch();
      }
      Assertions.assertEquals(values.length, ps.executeBatch().length);
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "select id, s from " + TABLE + " where id >= 100 and id < 200 order by id;")) {
      String[] expected = {quote, windowsPath, marks, chinese, keyword, commentMark, whitespace};
      int seen = 0;
      while (rs.next()) {
        Assertions.assertEquals(expected[seen], rs.getString(2),
                                "tunnel row " + seen + " (id=" + rs.getLong(1) + ") changed");
        seen++;
      }
      Assertions.assertEquals(expected.length, seen);
    }
  }

  @Test
  public void specialCharacterValuesRoundTripThroughTheConstantSqlPath() throws Exception {
    // With skipSqlInjectCheck the driver inlines the value itself, so the literal it builds has to
    // survive the server's own escape processing. On master a backslash was rewritten (a literal
    // "\n" became a newline, "\p" became "p") and a quote broke the statement outright:
    // ODPS-0130161 invalid token.
    String[] values = {"plain", "it's", "C:" + BS + "new_folder" + BS + "path", "a?b?c",
        "中文测试", "tab" + TAB + "end", "he said \"hi\""};
    for (int i = 0; i < values.length; i++) {
      try (PreparedStatement ps = literalConn.prepareStatement(
          "insert into " + TABLE + " (id, s) values (?, ?);")) {
        ps.setLong(1, 200L + i);
        ps.setString(2, values[i]);
        ps.execute();
      }
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "select id, s from " + TABLE + " where id >= 200 and id < 300 order by id;")) {
      int seen = 0;
      while (rs.next()) {
        Assertions.assertEquals(200L + seen, rs.getLong(1));
        Assertions.assertEquals(values[seen], rs.getString(2),
                                "the constant-SQL path rewrote row " + seen);
        seen++;
      }
      Assertions.assertEquals(values.length, seen, "not every value was written");
    }
  }

  @Test
  public void theInjectionHeuristicRejectsWithAnExplainedSqlException() throws Exception {
    // Master threw java.lang.IllegalArgumentException with an empty message from a method declared
    // to throw SQLException, so callers could neither catch it nor diagnose it.
    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, s) values (?, ?);")) {
      ps.setLong(1, 300L);
      ps.setString(2, "false', 'or 1=1");
      SQLException e = Assertions.assertThrows(SQLException.class, () -> ps.execute());
      Assertions.assertTrue(e.getMessage().contains("2"),
                            "the message should name the offending parameter: " + e.getMessage());
      Assertions.assertTrue(e.getMessage().contains("skipSqlInjectCheck"),
                            "the message should point at the documented opt-out: " + e.getMessage());
    }
    Assertions.assertEquals(0L, countRows("id = 300"), "a rejected value must not be written");
  }

  @Test
  public void decimalPrecisionIsCarriedOrRoundedAtTheColumnScale() throws Exception {
    BigDecimal exact = new BigDecimal("12345678901234567890.123456789");
    BigDecimal exponent = new BigDecimal("1E+2");
    BigDecimal overScale = new BigDecimal("1." + zeros(25) + "9");
    try (PreparedStatement ps = conn.prepareStatement(insertAll())) {
      ps.setLong(1, 400L);
      ps.setString(2, "decimal");
      ps.setString(3, "decimal");
      ps.setBigDecimal(4, exact);
      ps.setBytes(5, new byte[] {1});
      ps.setInt(6, 1);
      ps.executeUpdate();

      ps.clearParameters();
      ps.setLong(1, 401L);
      ps.setString(2, "decimal");
      ps.setString(3, "decimal");
      ps.setBigDecimal(4, exponent);
      ps.setBytes(5, new byte[] {1});
      ps.setInt(6, 1);
      ps.executeUpdate();

      ps.clearParameters();
      ps.setLong(1, 402L);
      ps.setString(2, "decimal");
      ps.setString(3, "decimal");
      ps.setBigDecimal(4, overScale);
      ps.setBytes(5, new byte[] {1});
      ps.setInt(6, 1);
      ps.executeUpdate();
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "select id, dec38 from " + TABLE + " where id between 400 and 402 order by id;")) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(0, exact.compareTo(rs.getBigDecimal(2)),
                              "a value inside the column scale must not be re-rounded");
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(0, new BigDecimal("100").compareTo(rs.getBigDecimal(2)),
                              "scientific notation must not lose its value");
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(0, new BigDecimal("1").compareTo(rs.getBigDecimal(2)),
                              "scale beyond the column is rounded by the server, not by the driver");
    }
    // The constant-SQL path renders a BigDecimal as a decimal literal; the value must survive too.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1 from (select 1 x) t;")) {
      ps.setBigDecimal(1, new BigDecimal("1E+2"));
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals(0, new BigDecimal("100").compareTo(rs.getBigDecimal(1)));
      }
    }
  }

  @Test
  public void bytesAreBinaryForTheTunnelAndTextForTheConstantSqlPath() throws Exception {
    byte[] payload = new byte[] {0x00, 0x01, (byte) 0xFF, (byte) 0x80, 'A', 'b'};
    try (PreparedStatement ps = conn.prepareStatement(insertAll())) {
      ps.setLong(1, 500L);
      ps.setString(2, "bytes");
      ps.setString(3, "bytes");
      ps.setBigDecimal(4, BigDecimal.ONE);
      ps.setBytes(5, payload);
      ps.setInt(6, 0);
      ps.executeUpdate();

      ps.clearParameters();
      ps.setLong(1, 501L);
      ps.setString(2, "bytes");
      ps.setString(3, "bytes");
      ps.setBigDecimal(4, BigDecimal.ONE);
      ps.setBytes(5, new byte[0]);
      ps.setInt(6, 0);
      ps.executeUpdate();
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select id, bin from "
                                       + TABLE + " where id in (500, 501) order by id;")) {
      Assertions.assertTrue(rs.next());
      Assertions.assertArrayEquals(payload, rs.getBytes(2), "BINARY payload changed");
      Assertions.assertTrue(rs.next());
      Object empty = rs.getObject(2);
      Assertions.assertNotNull(empty, "an empty byte[] came back as NULL");
      Assertions.assertFalse(rs.wasNull());
      Assertions.assertEquals(0, ((com.aliyun.odps.data.Binary) empty).data().length,
                              "an empty BINARY must stay empty");
    }
    // Documented asymmetry: on the constant-SQL path a byte[] is decoded with the connection
    // charset, i.e. it binds text, not binary. Control bytes cannot travel in a SQL literal.
    try (PreparedStatement ps = conn.prepareStatement(
        "select ? c1 from (select 1 x) t;")) {
      ps.setBytes(1, "abc".getBytes("UTF-8"));
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals("abc", rs.getString(1));
      }
    }
  }

  @Test
  public void streamAndLobBindingIsUniformlyUnsupported() throws Exception {
    // "Reader length boundaries": every stream / Reader / LOB overload, whatever the declared
    // length (including 0 and negative lengths, which must not be read as "unbounded"), answers
    // with SQLFeatureNotSupportedException and does not touch the value.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1 from (select 1 x) t;")) {
      byte[] bytes = new byte[] {'a', 'b', 'c'};
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setAsciiStream(1, new ByteArrayInputStream(bytes)));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setAsciiStream(1, new ByteArrayInputStream(bytes), 3));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setAsciiStream(1, new ByteArrayInputStream(bytes), 3L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setBinaryStream(1, new ByteArrayInputStream(bytes)));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setBinaryStream(1, new ByteArrayInputStream(bytes), 0));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setBinaryStream(1, new ByteArrayInputStream(bytes), -1L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setCharacterStream(1, new StringReader("abc")));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setCharacterStream(1, new StringReader("abc"), 0));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setCharacterStream(1, new StringReader("abc"), -1));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setCharacterStream(1, new StringReader("abc"), 3L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setNCharacterStream(1, new StringReader("abc")));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setNCharacterStream(1, new StringReader("abc"), 9L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setNString(1, "abc"));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setClob(1, (java.sql.Clob) null));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setClob(1, new StringReader("abc"), 3L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setBlob(1, new ByteArrayInputStream(bytes), 3L));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setUnicodeStream(1, new ByteArrayInputStream(bytes), 3));
      Calendar gmt = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setTimestamp(1, new java.sql.Timestamp(0L), gmt));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setDate(1, new java.sql.Date(0L), gmt));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                              () -> ps.setTime(1, new java.sql.Time(0L), gmt));
      Assertions.assertThrows(SQLFeatureNotSupportedException.class, ps::getParameterMetaData);
    }
  }

  static String zeros(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      sb.append('0');
    }
    return sb.toString();
  }

  // ---------- A2: statement lifecycle ----------

  @Test
  public void parameterIndexesOutsideThePlaceholderRangeAreRejected() throws Exception {
    // Master accepted any index (a HashMap has no opinion) and then bound NULL for the real
    // placeholder, so a single off-by-one silently changed the data.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1, ? c2 from (select 1 x) t;")) {
      Assertions.assertThrows(SQLException.class, () -> ps.setInt(0, 7));
      Assertions.assertThrows(SQLException.class, () -> ps.setInt(-3, 7));
      Assertions.assertThrows(SQLException.class, () -> ps.setString(3, "too far"));
      SQLException e = Assertions.assertThrows(SQLException.class, () -> ps.setLong(99, 9L));
      Assertions.assertTrue(e.getMessage().contains("99"), "name the bad index: " + e.getMessage());
      Assertions.assertTrue(e.getMessage().contains("2"), "name the real count: " + e.getMessage());

      // A rejected bind leaves the statement usable.
      ps.setInt(1, 1);
      ps.setString(2, "fine");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals(1, rs.getInt(1));
        Assertions.assertEquals("fine", rs.getString(2));
      }
    }
    // A '?' inside a literal or a comment is not a placeholder, so it must not widen the range.
    try (PreparedStatement ps = conn.prepareStatement(
        "select '?' c0, ? c1 from (select 1 x) t; -- trailing mark ?")) {
      ps.setString(1, "value");
      Assertions.assertThrows(SQLException.class, () -> ps.setString(2, "not a placeholder"));
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals("?", rs.getString(1));
        Assertions.assertEquals("value", rs.getString(2));
      }
    }
  }

  @Test
  public void executingWithAnUnboundParameterFailsInsteadOfWritingNull() throws Exception {
    long before = countRows("id = 600");
    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, s, i) values (?, ?, ?);")) {
      ps.setLong(1, 600L);
      ps.setString(2, "bound");
      ps.setInt(3, 3);
      ps.clearParameters();
      // Master wrote a row of three NULLs and reported "1 updated".
      SQLException e = Assertions.assertThrows(SQLException.class, ps::executeUpdate);
      Assertions.assertTrue(e.getMessage().contains("not been set"), e.getMessage());
    }
    Assertions.assertEquals(before, countRows("id = 600"), "no row may be written for unbound input");

    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, s, i) values (?, ?, ?);")) {
      ps.setLong(1, 601L);
      ps.setString(2, "partial");
      // index 3 never set: same promise on the batch path
      Assertions.assertThrows(SQLException.class, ps::addBatch);
    }

    try (PreparedStatement ps = conn.prepareStatement("select ? c1 from (select 1 x) t;")) {
      Assertions.assertThrows(SQLException.class, ps::executeQuery);
      ps.setString(1, "after the failure");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals("after the failure", rs.getString(1));
      }
    }
  }

  @Test
  public void theSameStatementRunsRepeatedlyWithNewValues() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, s) values (?, ?);")) {
      for (long id = 700L; id < 703L; id++) {
        ps.clearParameters();
        ps.setLong(1, id);
        ps.setString(2, "row-" + id);
        Assertions.assertEquals(1, ps.executeUpdate(), "executeUpdate() counts one row");
      }
    }
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "select id, s from " + TABLE + " where id >= 700 and id < 703 order by id;")) {
      for (long id = 700L; id < 703L; id++) {
        Assertions.assertTrue(rs.next(), "row for id " + id + " missing");
        Assertions.assertEquals(id, rs.getLong(1));
        Assertions.assertEquals("row-" + id, rs.getString(2));
      }
      Assertions.assertFalse(rs.next());
    }
    // The query path is reusable as well, and a re-bound parameter replaces the old value.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1, ? c2 from (select 1 x) t;")) {
      ps.setInt(1, 1);
      ps.setString(2, "first");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals(1, rs.getInt(1));
        Assertions.assertEquals("first", rs.getString(2));
      }
      ps.setInt(1, 2);
      ps.setString(2, "second");
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        Assertions.assertEquals(2, rs.getInt(1), "the second run saw a stale parameter");
        Assertions.assertEquals("second", rs.getString(2));
      }
    }
  }

  @Test
  public void operationsOnAClosedStatementFailWithSqlException() throws Exception {
    PreparedStatement ps = conn.prepareStatement("select ? c1 from (select 1 x) t;");
    ps.setString(1, "before close");
    ps.close();
    Assertions.assertTrue(ps.isClosed());
    // Master accepted the bind and then threw a NullPointerException at execute time.
    Assertions.assertThrows(SQLException.class, () -> ps.setString(1, "after close"));
    Assertions.assertThrows(SQLException.class, () -> ps.setInt(0, 1));
    Assertions.assertThrows(SQLException.class, ps::executeQuery);
    Assertions.assertThrows(SQLException.class, ps::execute);
    Assertions.assertThrows(SQLException.class, ps::executeUpdate);
    Assertions.assertThrows(SQLException.class, ps::addBatch);
    Assertions.assertThrows(SQLException.class, ps::executeBatch);
    // close() stays idempotent
    ps.close();
  }

  @Test
  public void anEmptyBatchIsAnEmptyUpdateCountArray() throws Exception {
    // Master threw IndexOutOfBoundsException for a column-list insert ("batchedRows.get(0)") while
    // a full-table insert returned int[0]: the same call, two different answers.
    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " (id, i) values (?, ?);")) {
      Assertions.assertEquals(0, ps.executeBatch().length);
      ps.clearBatch();
      Assertions.assertEquals(0, ps.executeBatch().length);
    }
    try (PreparedStatement ps = conn.prepareStatement(insertAll())) {
      Assertions.assertEquals(0, ps.executeBatch().length);
    }
  }

  @Test
  public void bindingAReleasedArrayFailsWithSqlException() throws Exception {
    Array released = conn.createArrayOf("string", new Object[] {"a", "b"});
    released.free();
    try (PreparedStatement ps = conn.prepareStatement(insertAll())) {
      // Since 3.10.14 a released OdpsArray answers getArray() with null; master then dereferenced
      // it and threw a NullPointerException.
      SQLException e = Assertions.assertThrows(SQLException.class, () -> ps.setArray(2, released));
      Assertions.assertTrue(e.getMessage().contains("free()"), e.getMessage());
    }
  }

  @Test
  public void batchAndTransactionSupportIsUnchanged() throws Exception {
    // Pinned, not changed: the driver has no transactions, and batch is the tunnel insert path.
    Assertions.assertTrue(conn.getAutoCommit());
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> conn.setAutoCommit(false));
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, conn::commit);
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, conn::rollback);
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> conn.rollback(null));
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> conn.setSavepoint());
    Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                            () -> conn.setSavepoint("sp"));
    Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> conn.releaseSavepoint(null));
    Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                            () -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    Assertions.assertTrue(conn.getAutoCommit(), "a rejected setAutoCommit() must not flip the flag");

    // Batch on a statement that is not a tunnel insert stays a clear SQLException.
    try (PreparedStatement ps = conn.prepareStatement("select ? c1 from (select 1 x) t;")) {
      ps.setString(1, "a");
      ps.addBatch();
      SQLException e = Assertions.assertThrows(SQLException.class, ps::executeBatch);
      Assertions.assertTrue(e.getMessage().contains("INSERT INTO"), e.getMessage());
    }
    // ... and a row count that does not match the table is reported with the numbers involved
    // (master surfaced the bare message "2" from an ArrayIndexOutOfBoundsException).
    try (PreparedStatement ps = conn.prepareStatement(
        "insert into " + TABLE + " values (?, ?);")) {
      ps.setLong(1, 800L);
      ps.setString(2, "short");
      SQLException e = Assertions.assertThrows(SQLException.class, ps::executeUpdate);
      Assertions.assertTrue(e.getMessage().contains("6"), "expected the column count: " + e.getMessage());
      Assertions.assertTrue(e.getMessage().contains("2"), "expected the bound count: " + e.getMessage());
    }
    Assertions.assertEquals(0L, countRows("id = 800"));
  }
}
