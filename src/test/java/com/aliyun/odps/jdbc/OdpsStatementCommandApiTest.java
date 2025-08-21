package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.google.common.collect.ImmutableMap;

public class OdpsStatementCommandApiTest {

  static final String tableName = "commandapi_test";

  static Connection conn;

  @BeforeAll
  public static void prepare() throws Exception {
    conn = TestUtils.getConnection(
            ImmutableMap.of("enableCommandApi", "true", "interactiveMode", "true"));

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

}
