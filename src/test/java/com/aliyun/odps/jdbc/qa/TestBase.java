package com.aliyun.odps.jdbc.qa;

import com.alibaba.druid.util.JdbcUtils;
import com.aliyun.odps.jdbc.BVTConf;
import com.aliyun.odps.jdbc.OdpsDriver;
import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;

/*
    Created by ximo.zy 2015/07/23
 */
public class TestBase extends TestCase {

    Properties info;

    OdpsDriver driver;

    protected Connection conn;

    DatabaseMetaData metaData;

    String url;

    protected void setUp() throws Exception {
        driver = OdpsDriver.instance;
        info = new Properties();

        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        info.put("project_name", BVTConf.getProjectName());

        url = BVTConf.getEndPoint();

        conn = driver.connect("jdbc:odps:" + url, info);
        metaData = conn.getMetaData();
    }

    protected void tearDown() throws Exception {
        conn.close();
    }


}
