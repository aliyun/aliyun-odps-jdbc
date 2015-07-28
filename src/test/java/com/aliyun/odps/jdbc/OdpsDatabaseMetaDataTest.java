package com.aliyun.odps.jdbc;

import com.alibaba.druid.util.JdbcUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import junit.framework.TestCase;

public class OdpsDatabaseMetaDataTest extends TestCase {

  DatabaseMetaData databaseMetaData;

  protected void setUp() throws Exception {
    databaseMetaData = OdpsConnectionFactory.getInstance().conn.getMetaData();
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
