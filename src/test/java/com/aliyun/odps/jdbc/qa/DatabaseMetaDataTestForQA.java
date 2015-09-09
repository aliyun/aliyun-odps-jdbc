package com.aliyun.odps.jdbc.qa;

import org.junit.Ignore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/*
 * DatabaseMetaData test
 */
public class DatabaseMetaDataTestForQA extends TestBase {

    static DatabaseMetaData databaseMetaData;

    @BeforeClass
    public static void setUp() throws Exception {
        databaseMetaData = conn.getMetaData();
        statement.executeUpdate(
                "create table if not exists odps_jdbc_t1(id bigint,name string)");
        statement.executeUpdate(
                "create view if not exists odps_jdbc_view_t1(a,b) comment 'a view ' as select * from odps_jdbc_t1");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        statement.executeUpdate(
                "drop table if exists odps_jdbc_t1");
        statement.executeUpdate(
                "drop view if exists odps_jdbc_view_t1");
        statement.close();
        conn.close();
    }


    @Test
    public void testGetTables() throws Exception {
        ResultSet rsTable = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});
        ResultSet rsView = databaseMetaData.getTables(null, null, null, new String[]{"VIEW"});

        printRs(rsTable);
        printRs(rsView);

        ResultSetMetaData tableMeta = rsTable.getMetaData();
        ResultSetMetaData viewMeta = rsView.getMetaData();

        Assert.assertNotEquals(rsTable.getObject(3), rsView.getObject(3));

        rsTable.close();
        rsView.close();
    }

    //Bug : http://k3.alibaba-inc.com/issue/6397273
    // @Test
    @Ignore
    public void testGetColumns() throws Exception {
        ResultSet rs = databaseMetaData.getColumns(null, null, "odps_jdbc_t1", "id");
        printRs(rs);
        Assert.assertTrue(rs != null && rs.next() && rs.getObject(3).equals("odps_jdbc_t1") && rs.getObject(4).equals("id"));
    }


    // Bug : http://k3.alibaba-inc.com/issue/6396925
    // @Test
    @Ignore
    public void testGetFunctions() throws Exception {
        ResultSet rs = databaseMetaData.getFunctions(null, null, "notexistfunctionName");
        printRs(rs);
        Assert.assertFalse(rs != null && rs.next());
        rs.close();
    }


    @Test
    public void testGetPrimaryKeys() throws Exception {
        ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, "odps_jdbc_t1");
        printRs(rs);
        Assert.assertFalse(rs != null && rs.next());
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
