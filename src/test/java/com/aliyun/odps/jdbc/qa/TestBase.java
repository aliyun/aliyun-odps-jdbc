package com.aliyun.odps.jdbc.qa;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
                setUp();
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

    public static void setUp() throws Exception {
        statement.executeUpdate(
                "drop table if exists odps_jdbc_t10000;");
        statement.executeUpdate(
                "create table if not exists odps_jdbc_t10000(id bigint,name string);");
        statement.executeUpdate(
                "insert into table odps_jdbc_t10000 select count(*),count(*) from odps_jdbc_t10000;");
        statement.executeUpdate(
                "insert into table odps_jdbc_t10000 select count(*),count(*) from odps_jdbc_t10000;");
        statement.executeUpdate(
                "insert into table odps_jdbc_t10000 select count(*),count(*) from odps_jdbc_t10000;");
    }


    public static void tearDown() throws Exception {
        statement.executeUpdate(
                "drop table if exists odps_jdbc_t10000");
        statement.close();
        conn.close();
    }

    // Print resultSet
    public void printRs(ResultSet resultSet) throws Exception {
        ResultSetMetaData meta = resultSet.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.print("\t" + meta.getColumnName(i));
        }
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            System.out.print("\t" + meta.getColumnTypeName(i));
        }
        System.out.println();

        while (resultSet.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print("\t" + resultSet.getObject(i));
            }
            System.out.println();
        }
    }
}

