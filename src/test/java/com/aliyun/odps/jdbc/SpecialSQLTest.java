package com.aliyun.odps.jdbc;

import java.sql.Statement;

import org.junit.Test;

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
