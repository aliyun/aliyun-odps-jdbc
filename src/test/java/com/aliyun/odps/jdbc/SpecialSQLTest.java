package com.aliyun.odps.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Statement;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SpecialSQLTest {

    @Test
    public void testEndWithSpace() throws Exception {
        String sql = "select * from test; ";
        TestManager instance = TestManager.getInstance();
        Statement statement = instance.conn.createStatement();
        try {
            statement.executeQuery(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        statement.close();
    }

}
