package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Assert;

import com.alibaba.druid.util.JdbcUtils;
import com.aliyun.odps.jdbc.OdpsDriver;

public class OdpsSetSchemaTest extends TestCase {

    public void test_0() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;

        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        Connection conn = driver.connect("jdbc:odps:service-corp.odps.aliyun-inc.com/secdw_dev", info);

        {
            String catalog = conn.getCatalog();
            Assert.assertEquals("secdw_dev", catalog);
        }
        
        conn.setCatalog("secods");

        {
            String catalog = conn.getCatalog();
            Assert.assertEquals("secods", catalog);
        }
        
        DatabaseMetaData meta = conn.getMetaData();
        
        Assert.assertSame(conn, meta.getConnection());
        
//        {
//            ResultSet rs = meta.getTables(null, null, null, null);
//            JdbcUtils.printResultSet(rs);
//            rs.close();
//        }
        
        {
            ResultSet rs = meta.getFunctions(null, null, null);
            JdbcUtils.printResultSet(rs);
            rs.close();
        }
        
        conn.close();
    }
}
