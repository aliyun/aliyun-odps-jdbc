package com.aliyun.odps.jdbc.qa;

import junit.framework.*;
import org.junit.*;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * Created by ximo.zy on 2015/9/10.
 */
public class ResultSetTestForQA extends TestBase {
    static Statement stmtSens;

    //SQLFeatureNotSupportedException
    //@Test
    @Ignore
    public void testConCommit() throws Exception {
        //SQLFeatureNotSupportedException
        // stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = stmtSens.executeQuery("SELECT * FROM odps_jdbc_t10000");
        conn.commit();
    }

    //Bug : http://k3.alibaba-inc.com/issue/6411858?versionId=1141800
    @Test
    public void testSetFetchDirectionReverse() throws Exception {
        stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = stmtSens.executeQuery("SELECT * FROM odps_jdbc_t10000");
        result.setFetchDirection(ResultSet.FETCH_REVERSE);
        result.previous();

        while (result.next()) {
            System.out.println("---111---" + result.getString(1));
            System.out.println("---111---" + result.getString(2));
        }
        Assert.assertEquals(true, result.isAfterLast());
        while (result.next()) {
            System.out.println("---222---" + result.getString(1));
            System.out.println("---222---" + result.getString(2));
        }
    }


    //Only FETCH_FORWARD and FETCH_REVERSE is valid
    //@Test
    @Ignore
    public void testSetFetchDirectionUnkown() throws Exception {
        stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = stmtSens.executeQuery("SELECT * FROM odps_jdbc_t10000");
        result.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    }

    @Test
    public void testSetFetchSize() throws Exception {
        stmtSens = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = stmtSens.executeQuery("SELECT * FROM odps_jdbc_t10000");
        result.setFetchSize(2);
        Assert.assertEquals(2, result.getFetchSize());

        while (result.next()) {
            System.out.print(result.getString(1) + " ");
            System.out.print(result.getString(2) + " ");
            // result.updateString(1,"xxx");
        }
    }
}
