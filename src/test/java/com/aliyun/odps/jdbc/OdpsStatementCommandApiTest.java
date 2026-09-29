package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

public class OdpsStatementCommandApiTest {

  static final String tableName = "commandapi_test";

  static Connection conn;

  @BeforeAll
  public static void prepare() throws Exception {
    conn = TestUtils.getConnection(
            ImmutableMap.of("enableCommandApi", "true", "interactiveMode", "false"));

    Statement statement = conn.createStatement();
    statement.execute("drop table if exists " + tableName);
    statement.execute("create table " + tableName + " (key string, value datetime);");
  }

  @AfterAll
  public static void after() throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("drop table if exists " + tableName);
  }

  @Test
  public void showCreateTableTest() throws SQLException {
    Statement statement = conn.createStatement();

    String sql = "show create table " + tableName;
    ResultSet resultSet;

    resultSet = statement.executeQuery(sql);
    while (resultSet.next()) {
      int count = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(resultSet.getMetaData().getColumnName(i) + " : " + resultSet.getString(i));
      }
    }
  }

  @Test
  public void showSchemaTest() throws SQLException {
    Statement statement = conn.createStatement();

    String sql = "show schemas ;";
    ResultSet resultSet;

    resultSet = statement.executeQuery(sql);
    while (resultSet.next()) {
      int count = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(resultSet.getMetaData().getColumnName(i) + " : " + resultSet.getString(i));
      }
    }
  }

  @Test
  public void commandApiTest() throws SQLException {
    Statement stmt = conn.createStatement();

    String sql;
    ResultSet res;

    res = stmt.executeQuery("desc " + tableName + ";");
    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }

    sql = "whoami;";
    stmt.execute(sql);
    res = stmt.getResultSet();

    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }

    sql =
        String.format("insert into %s values ('testnow' , datetime('2022-07-10 10:10:00'));",
                      tableName);
    int updateCount = stmt.executeUpdate(sql);
    System.out.println(updateCount);
    System.out.println("update sql resultSet: " + stmt.getResultSet());
    System.out.println(((OdpsStatement) stmt).getLogViewUrl());


    sql = "select * from " + tableName;
    res = stmt.executeQuery(sql);
    while (res.next()) {
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        System.out.println(res.getMetaData().getColumnName(i) + " : " + res.getString(i));
      }
    }
    System.out.println(((OdpsStatement) stmt).getLogViewUrl());

  }

  /**
   * Regression for the driver-side NullPointerException on `desc <table>`: this connection
   * (enableCommandApi=true, interactiveMode=false, enableLimit=false) is exactly the shape that
   * used to hit it. `desc <table>` is a synchronous Command API statement, so it creates no
   * instance and must be served by the SQLExecutor result path instead of the offline instance
   * tunnel, which dereferences the instance without checking it.
   */
  @Test
  public void describeTableWithoutInstanceTest() throws SQLException {
    Statement stmt = conn.createStatement();

    ResultSet res;
    try {
      res = stmt.executeQuery("desc " + tableName + ";");
    } catch (NullPointerException e) {
      throw new AssertionError("`desc <table>` must not leak a NullPointerException out of the "
                                + "driver, the caller gets neither an error code nor an instance"
                                + " id to locate the failure", e);
    }

    // The precondition of the bug: no instance was created, so the tunnel path cannot apply.
    assertNull(((OdpsStatement) stmt).getExecuteInstance(),
               "`desc <table>` is a synchronous command and should not create an instance");

    StringBuilder dumped = new StringBuilder();
    int rows = 0;
    while (res.next()) {
      rows++;
      int count = res.getMetaData().getColumnCount();
      for (int i = 1; i <= count; i++) {
        dumped.append(res.getMetaData().getColumnName(i)).append(" : ")
              .append(res.getString(i)).append('\n');
      }
    }
    System.out.println(dumped);

    assertTrue(rows > 0, "`desc <table>` should return the table metadata, got 0 rows");
    assertTrue(dumped.indexOf("key") >= 0, "desc output should carry the column `key`: " + dumped);
    assertTrue(dumped.indexOf("value") >= 0,
               "desc output should carry the column `value`: " + dumped);
  }

  /**
   * A command that fails on the server side must come back as a locatable SQLException, not as a
   * runtime exception escaping from the result path.
   */
  @Test
  public void describeMissingTableThrowsSQLExceptionTest() throws SQLException {
    String missing = tableName + "_missing_";
    Statement stmt = conn.createStatement();

    // Deliberately catching Exception: if the unchecked NullPointerException this test
    // regression-guards ever comes back, it must be reported as "wrong exception type" instead
    // of escaping the test method.
    Exception thrown =
        assertThrows(Exception.class, () -> stmt.executeQuery("desc " + missing + ";"));
    assertTrue(thrown instanceof SQLException,
               "a failing command must surface as a locatable SQLException, but got "
               + thrown.getClass().getName() + ": " + thrown.getMessage());
    assertNotNull(thrown.getMessage(), "the SQLException should carry the server error");
    System.out.println("desc missing table SQLException: " + thrown.getMessage());
  }

  /**
   * A command that yields no rows must come back as a normal empty result set. Still the same
   * null-instance result path -- `show partitions` is synchronous too -- just with nothing in it.
   */
  @Test
  public void commandWithoutRowsReturnsEmptyResultSetTest() throws SQLException {
    String partitioned = tableName + "_part";
    Statement stmt = conn.createStatement();
    try {
      stmt.execute("drop table if exists " + partitioned);
      stmt.execute("create table " + partitioned + " (key string) partitioned by (ds string)");

      ResultSet res;
      try {
        res = stmt.executeQuery("show partitions " + partitioned + ";");
      } catch (NullPointerException e) {
        throw new AssertionError("a command returning nothing must not throw "
                                 + "NullPointerException", e);
      }
      assertNull(((OdpsStatement) stmt).getExecuteInstance(),
                 "`show partitions` is a synchronous command and should not create an instance");

      int rows = 0;
      while (res.next()) {
        rows++;
      }
      System.out.println("show partitions '<empty partitioned table>' rows: " + rows);
      assertEquals(0, rows, "a partitioned table without partitions should return no rows");
    } finally {
      stmt.execute("drop table if exists " + partitioned);
    }
  }

}
