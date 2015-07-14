package com.aliyun.odps.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

import junit.framework.TestCase;

public class OdpsPreparedStatementTest extends TestCase {

    protected PreparedStatement pstmt;
    protected long unixtime;

    protected void setUp() throws Exception {
        OdpsDriver driver = OdpsDriver.instance;

        Properties info = new Properties();
        info.put("access_id", BVTConf.getAccessId());
        info.put("access_key", BVTConf.getAccessKey());
        info.put("project_name", BVTConf.getProjectName());
        info.put("end_point", BVTConf.getEndPoint());

        Connection conn = driver.connect("jdbc:odps:", info);
        pstmt = conn.prepareStatement("select ? whatever from dual;");

        unixtime = new java.util.Date().getTime();
    }

    protected void tearDown() throws Exception {
        pstmt.close();
    }

    public void test_set_int() throws Exception {
        int x = Integer.MAX_VALUE;
        pstmt.setInt(1, x);

        ResultSet rs = pstmt.executeQuery();

        int y = 0;
        while (rs.next()) {
            y = rs.getInt(1);
        }
        assertEquals(x, y);
    }

    public void test_set_short() throws Exception {
        short x = Short.MAX_VALUE;
        pstmt.setShort(1, x);

        ResultSet rs = pstmt.executeQuery();

        short y = 0;
        while (rs.next()) {
            y = rs.getShort(1);
        }
        assertEquals(x, y);
    }

    public void test_set_long() throws Exception {
        long x = Long.MAX_VALUE;
        pstmt.setLong(1, x);

        ResultSet rs = pstmt.executeQuery();

        long y = 0;
        while (rs.next()) {
            y = rs.getLong(1);
        }
        assertEquals(x, y);
    }

    public void test_set_boolean() throws Exception {
        Boolean x = Boolean.TRUE;
        pstmt.setBoolean(1, x);

        ResultSet rs = pstmt.executeQuery();

        Boolean y = Boolean.FALSE;
        while (rs.next()) {
            y = rs.getBoolean(1);
        }
        assertEquals(x, y);
    }

    public void test_set_big_decimal() throws Exception {
        BigDecimal x = BigDecimal.TEN;
        pstmt.setBigDecimal(1, x);

        ResultSet rs = pstmt.executeQuery();

        BigDecimal y = BigDecimal.ONE;
        while (rs.next()) {
            y = rs.getBigDecimal(1);
        }
        assertEquals(x, y);
    }

    public void test_set_byte() throws Exception {
        byte x = Byte.MAX_VALUE;
        pstmt.setByte(1, x);

        ResultSet rs = pstmt.executeQuery();

        byte y = 0;
        while (rs.next()) {
            y = rs.getByte(1);
        }
        assertEquals(x, y);
    }

    public void test_set_float() throws Exception {
        float x = Float.MAX_VALUE;
        pstmt.setFloat(1, x);

        ResultSet rs = pstmt.executeQuery();

        float y = 0;
        while (rs.next()) {
            y = rs.getFloat(1);
        }
        assertEquals(x, y);
    }

    public void test_set_double() throws Exception {
        double x = Double.MAX_VALUE;
        pstmt.setDouble(1, x);

        ResultSet rs = pstmt.executeQuery();

        double y = 0.0E00;
        while (rs.next()) {
            y = rs.getDouble(1);
        }
        assertEquals(x, y);
    }

    public void test_set_date() throws Exception {
        Date x = new Date(unixtime);
        pstmt.setDate(1, x);

        ResultSet rs = pstmt.executeQuery();

        Date y = new Date(0);
        while (rs.next()) {
            y = rs.getDate(1);
        }
        assertEquals(x, y);
    }

    public void test_set_time() throws Exception {
        Time x = new Time(unixtime);
        pstmt.setTime(1, x);

        ResultSet rs = pstmt.executeQuery();

        Time y = new Time(0);
        while (rs.next()) {
            y = rs.getTime(1);
        }
        assertEquals(x, y);
    }

    public void test_set_timestamp() throws Exception {
        Timestamp x = new Timestamp(unixtime);
        pstmt.setTimestamp(1, x);

        ResultSet rs = pstmt.executeQuery();

        Timestamp y = new Timestamp(0);
        while (rs.next()) {
            y = rs.getTimestamp(1);
        }
        assertEquals(x, y);
    }

    public void test_set_string() throws Exception {
        String x = "hello";
        pstmt.setString(1, x);

        ResultSet rs = pstmt.executeQuery();

        String y = "haha";
        while (rs.next()) {
            y = rs.getString(1);
        }
        assertEquals(x, y);
    }
}

