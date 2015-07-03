package com.aliyun.odps.jdbc;

import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Assert;

import com.aliyun.odps.jdbc.impl.OdpsConnection;
import com.aliyun.odps.jdbc.impl.OdpsStatement;

public class ExecuteUpdateTest extends TestCase {

  public void test_update() throws Exception {
    OdpsDriver driver = OdpsDriver.instance;

    Properties info = new Properties();
    info.put("access_id", BVTConf.getAccessId());
    info.put("access_key", BVTConf.getAccessKey());
    OdpsConnection conn =
        (OdpsConnection) driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw", info);

    String sql = "insert into table secdw_dev.test_wenshao_0622 select count(*) from dual;";
    OdpsStatement stmt = conn.createStatement();
    int updateCount = stmt.executeUpdate(sql);
    Assert.assertEquals(1, updateCount);
    stmt.close();
    conn.close();

  }
}
