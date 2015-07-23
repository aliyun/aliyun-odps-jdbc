package com.aliyun.odps.jdbc;

import com.alibaba.druid.util.JdbcUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsDatabaseMetaDataTest extends TestCase {

  protected Connection conn;
  DatabaseMetaData databaseMetaData;

  protected void setUp() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    info.put("project_name", BVTConf.getProjectName());
    String url = BVTConf.getEndPoint();
    conn = driver.connect("jdbc:odps:" + url, info);
    databaseMetaData = conn.getMetaData();
  }

  protected void tearDown() throws Exception {
    conn.close();
  }

  public void testGetTables() throws Exception {
    ResultSet rs = databaseMetaData.getTables(null, null, null, null);
    JdbcUtils.printResultSet(rs);
    rs.close();
  }

  public void testGetFunctions() throws Exception {
    ResultSet rs = databaseMetaData.getFunctions(null, null, null);
    JdbcUtils.printResultSet(rs);
    rs.close();
  }
}
