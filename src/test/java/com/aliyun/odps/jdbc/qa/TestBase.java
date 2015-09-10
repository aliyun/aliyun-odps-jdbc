package com.aliyun.odps.jdbc.qa;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/*
 * 测试基类
 */
public class TestBase {
    static Connection conn;

    static TestBase testBase;

    static Statement statement;

    static {
        if (testBase == null) {
            testBase = TestBase.getInstance();
        }
        if (conn == null) {
            try {
                conn = testBase.getConnection();
                statement = conn.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static TestBase getInstance() {
        return new TestBase();
    }

    public static Connection getConnection() throws Exception {
        Properties properties = new Properties();

        InputStream inputStream =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("bvt_conf.properties");
        properties.load(inputStream);

        Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

        String endpoint = properties.getProperty("end_point");
        String project = properties.getProperty("project_name");
        String username = properties.getProperty("access_id");
        String password = properties.getProperty("access_key");

        String url = "jdbc:odps:" + endpoint + "?project=" + project + "&lifecycle=3";
        System.out.print("----url-----" + url);
        conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    @Test
    public void testGetProcedures() throws Exception {
        Assert.assertTrue(testBase != null && conn != null);
    }


}

