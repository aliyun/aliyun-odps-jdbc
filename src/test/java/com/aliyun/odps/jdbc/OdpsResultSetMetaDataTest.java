package com.aliyun.odps.jdbc;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

public class OdpsResultSetMetaDataTest extends TestCase {

    protected Connection conn;
    protected Statement stmt;
    protected ResultSet rs;
    protected ResultSetMetaData rsmd;

    protected void setUp() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;

        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        info.put("project_name", BVTConf.getProjectName());
        String url = BVTConf.getEndPoint();

        conn = driver.connect("jdbc:odps:" + url, info);
        stmt = conn.createStatement();
        String sql = "select 'yichao' name, true male, 25 age, 173.5 height, "
            + "cast('2015-07-09 11:11:11' as datetime) day from dual;";
        rs = stmt.executeQuery(sql);
        rsmd = rs.getMetaData();
    }

    protected void tearDown() throws Exception {
        stmt.close();
        conn.close();
        rs.close();
    }

    public void testGetColumnCount() throws Exception {
        assertEquals(5, rsmd.getColumnCount());
    }

    public void testGetColumnName() throws Exception {
        assertEquals("name", rsmd.getColumnName(1));
        assertEquals("male", rsmd.getColumnName(2));
        assertEquals("age", rsmd.getColumnName(3));
        assertEquals("height", rsmd.getColumnName(4));
        assertEquals("day", rsmd.getColumnName(5));
    }

    public void testGetColumnType() throws Exception {
        assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
        assertEquals(Types.BOOLEAN, rsmd.getColumnType(2));
        assertEquals(Types.BIGINT, rsmd.getColumnType(3));
        assertEquals(Types.DOUBLE, rsmd.getColumnType(4));
        assertEquals(Types.DATE, rsmd.getColumnType(5));
    }
}
