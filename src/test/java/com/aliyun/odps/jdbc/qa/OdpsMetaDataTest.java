package com.aliyun.odps.jdbc.qa;

import junit.framework.Assert;

import java.sql.DatabaseMetaData;

/*
    Created by ximo.zy 2015/07/23
 */
public class OdpsMetaDataTest extends TestBase {

    public void testGetTables() throws Exception {
        DatabaseMetaData metadata = conn.getMetaData();
        Assert.assertEquals(metadata.getURL(),this.url);
        System.out.println(metadata.getUserName());
       /* //getDriverName()：返回驱动驱动程序的名称
        System.out.println(metadata.getDriverName());
        //getDriverVersion()：返回驱动程序的版本号
        System.out.println(metadata.getDriverVersion());
        //isReadOnly()：返回一个boolean值，指示数据库是否只允许读操作
        System.out.println(metadata.isReadOnly());
        //getDatabaseProductName()：返回数据库的产品名称
        System.out.println(metadata.getDatabaseProductName());*/
    }
}
