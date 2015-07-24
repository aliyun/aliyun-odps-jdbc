package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsStatementTest extends TestCase {

  protected Connection conn;
  protected Statement stmt;

  protected void setUp() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    info.put("project_name", BVTConf.getProjectName());
    String url = BVTConf.getEndPoint();

    conn = driver.connect("jdbc:odps:" + url, info);
    stmt = conn.createStatement();
    stmt.executeUpdate(
        "create table if not exists yichao_test_table_output(id bigint);");

  }

  protected void tearDown() throws Exception {
    stmt.executeUpdate("drop table if exists yichao_test_table_output;");
    stmt.close();
    conn.close();
  }

  public void testExecuteUpdate() throws Exception {
    String sql =
        "insert into table yichao_test_table_output select * from yichao_test_table_input;";
    int updateCount = stmt.executeUpdate(sql);
    assertEquals(100*10000, updateCount);
  }

  public void testExecuteQuery() throws Exception {
    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);

    int i = 0;
    while (rs.next()) {
      assertEquals(i, rs.getInt(1));
      i++;
    }
  }
}
