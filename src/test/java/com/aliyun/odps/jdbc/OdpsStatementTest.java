package com.aliyun.odps.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsStatementTest extends TestCase {

    protected Connection conn;
    protected Statement stmt;

    protected void setUp() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;

        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        info.put("project_name", BVTConf.getProjectName());
        String url = BVTConf.getEndPoint();

        conn = driver.connect("jdbc:odps:" + url, info);
        stmt = conn.createStatement();

        stmt.executeUpdate(
            "create table if not exists yichao_test_table_input (name string, id bigint);");
        stmt.executeUpdate(
            "create table if not exists yichao_test_table_output (name string, id bigint);");

        String sql = "insert into table yichao_test_table_input select '%s' name, %d id from dual ;";
        stmt.executeUpdate(String.format(sql, "batman", 100));
        stmt.executeUpdate(String.format(sql, "superman", 200));
        stmt.executeUpdate(String.format(sql, "spiderman", 300));
        stmt.executeUpdate(String.format(sql, "ironman", 400));
    }

    protected void tearDown() throws Exception {
        stmt.executeUpdate("drop table if exists yichao_test_table_input;");
        stmt.executeUpdate("drop table if exists yichao_test_table_output;");
        stmt.close();
        conn.close();
    }

    public void testExecuteUpdate() throws Exception {
        String sql =
            "insert into table yichao_test_table_output select * from yichao_test_table_input;";
        int updateCount = stmt.executeUpdate(sql);
        assertEquals(4, updateCount);
    }

    public void testExecuteQuery() throws Exception {
        String sql = "select * from yichao_test_table_input order by id limit 4;";
        ResultSet rs = stmt.executeQuery(sql);
        String[] names = {"batman", "superman", "spiderman", "ironman"};

        int i = 0;
        while (rs.next()) {
            assertEquals(names[i], rs.getString(1));
            i++;
        }
    }
}
