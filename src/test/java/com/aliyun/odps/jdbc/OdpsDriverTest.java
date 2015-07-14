package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsDriverTest extends TestCase {

  public void test_0() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    info.put("project_name", BVTConf.getProjectName());
    String url = BVTConf.getEndPoint();

    Connection conn = driver.connect("jdbc:odps:" + url, info);

    String sql = "select * from (select 1 id, 'steven jobs' name from dual union all "
            + "select 2 id,'bill gates' name from dual) x;";
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    rs.close();
    stmt.close();
  }
}
