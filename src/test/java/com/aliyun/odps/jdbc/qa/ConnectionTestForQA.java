package com.aliyun.odps.jdbc.qa;

import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by ximo.zy on 2015/9/14.
 */
public class ConnectionTestForQA extends TestBase {

    @Test
    public void testPrepareStatement() throws Exception {
        String sql = "select * from odps_jdbc_t10000 where id=?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        int id = -1;
        pstmt.setInt(1, id);
        ResultSet rs = pstmt.executeQuery();
        Assert.assertEquals(false, rs.next());
    }
}
