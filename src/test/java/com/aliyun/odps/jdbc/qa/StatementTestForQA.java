package com.aliyun.odps.jdbc.qa;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

/**
 * Created by ximo.zy on 2015/9/11.
 */
public class StatementTestForQA extends TestBase {
    @BeforeClass
    public static void setUp() throws Exception {

    }

    @AfterClass
    public static void tearDown() throws Exception {
        statement.close();
        conn.close();
    }

    @Test
    public void testSetFetchSize() throws Exception {
        statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = statement.executeQuery("SELECT * FROM t1");
        result.setFetchSize(100000);
        Assert.assertEquals(100000, result.getFetchSize());

    }

    @Test
    public void testRelative() throws Exception {
        statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = statement.executeQuery("SELECT * FROM odps_jdbc_t10000");
        Assert.assertEquals(true, result.relative(3));
        Assert.assertEquals(true, result.relative(-1));
        Assert.assertEquals(false, result.isBeforeFirst());
        Assert.assertEquals(false, result.isAfterLast());
        Assert.assertEquals(false, result.isFirst());
        Assert.assertEquals(false, result.isLast());
        Assert.assertEquals(2, result.getRow());
        result.getType();
        result.wasNull();
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
