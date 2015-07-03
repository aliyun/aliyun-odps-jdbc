package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

public class DescTest extends TestCase {

  public void test_0() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    Connection conn = driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw", info);

    String sql = "desc dual;";
    Statement stmt = conn.createStatement();
    stmt.executeQuery(sql);
    // rs.close();
    stmt.close();
  }
}
