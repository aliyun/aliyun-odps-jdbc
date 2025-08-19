package com.aliyun.odps.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import com.aliyun.odps.jdbc.utils.TestUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SpecialSQLTest {

    @Test
    public void testEndWithSpace() throws Exception {
        String sql = "select * from test; ";
        Connection conn = TestUtils.getConnection();
        Statement statement = conn.createStatement();
        try {
            statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        statement.close();
    }

}
