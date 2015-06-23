package com.aliyun.odps.jdbc;

import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Assert;

import junit.framework.TestCase;

import com.alibaba.druid.util.JdbcUtils;
import com.aliyun.odps.jdbc.impl.OdpsConnection;
import com.aliyun.odps.jdbc.impl.OdpsStatement;

public class ExecuteTest extends TestCase {

    public void test_execute_for_query() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;
        
        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        OdpsConnection conn = (OdpsConnection) driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw", info);
        
        String sql = "select count(*) from dual;";
        OdpsStatement stmt = conn.createStatement();
        boolean firstResultSet = stmt.execute(sql);
        Assert.assertTrue(firstResultSet);
        Assert.assertEquals(-1, stmt.getUpdateCount());
        
        ResultSet rs = stmt.getResultSet();
        
        JdbcUtils.printResultSet(rs);
        
        rs.close();
        
        stmt.close();
        conn.close();
    }
    
    public void test_execute_for_update() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;
        
        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        OdpsConnection conn = (OdpsConnection) driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw", info);
        
        String sql = "insert into table secdw_dev.test_wenshao_0622 select count(*) from dual;";
        OdpsStatement stmt = conn.createStatement();
        stmt.execute(sql);
        
        Assert.assertNull(stmt.getResultSet());
        Assert.assertEquals(1, stmt.getUpdateCount());
        
        stmt.close();
        conn.close();
    }
}
