package com.aliyun.odps.jdbc;

import java.sql.ResultSet;
import java.util.Properties;

import junit.framework.TestCase;

import com.alibaba.druid.util.JdbcUtils;
import com.aliyun.odps.jdbc.impl.OdpsConnection;
import com.aliyun.odps.jdbc.impl.OdpsStatement;

public class ExecuteQueryTest extends TestCase {

  public void test_update() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    OdpsConnection conn =
        (OdpsConnection) driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw", info);

    String sql = "select * from dual;";
    OdpsStatement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    JdbcUtils.printResultSet(rs);
    rs.close();
    stmt.close();
    conn.close();

  }
}
