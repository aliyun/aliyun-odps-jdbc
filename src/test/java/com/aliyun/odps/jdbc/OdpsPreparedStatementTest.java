/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsPreparedStatementTest {

  @AfterClass
  public static void tearDown() throws Exception {
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testSetAll() throws Exception {
    PreparedStatement pstmt;
    pstmt = OdpsConnectionFactory.getInstance().conn.prepareStatement(
        "select ? c1, ? c2, ? c3, ? c4, ? c5, ? c6, "
        + "? c7, ? c8, ? c9, ? c10, ? c11, ? c12, ? c13 from dual;");
    long unixtime = new java.util.Date().getTime();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    pstmt.setBigDecimal(1, BigDecimal.TEN);
    pstmt.setBoolean(2, Boolean.TRUE);
    pstmt.setByte(3, Byte.MAX_VALUE);
    pstmt.setDate(4, new Date(unixtime));
    pstmt.setDouble(5, Double.MAX_VALUE);
    pstmt.setFloat(6, Float.MAX_VALUE);
    pstmt.setInt(7, Integer.MAX_VALUE);
    pstmt.setObject(8, 0.314);
    pstmt.setLong(9, Long.MAX_VALUE);
    pstmt.setShort(10, Short.MAX_VALUE);
    pstmt.setString(11, "hello");
    pstmt.setTime(12, new Time(unixtime));
    pstmt.setTimestamp(13, new Timestamp(unixtime));

    {
      ResultSet rs = pstmt.executeQuery();
      rs.next();
      Assert.assertEquals(BigDecimal.TEN, rs.getBigDecimal(1));
      Assert.assertEquals(Boolean.TRUE, rs.getBoolean(2));
      Assert.assertEquals(Byte.MAX_VALUE, rs.getByte(3));
      Assert.assertEquals(new Date(unixtime).toString(), rs.getDate(4).toString());
      Assert.assertEquals(Double.MAX_VALUE, rs.getDouble(5), 0);
      Assert.assertEquals(Float.MAX_VALUE, rs.getFloat(6), 0);
      Assert.assertEquals(Integer.MAX_VALUE, rs.getInt(7));
      Assert.assertEquals(0.314, rs.getDouble(8), 0);
      Assert.assertEquals(Long.MAX_VALUE, rs.getLong(9));
      Assert.assertEquals(Short.MAX_VALUE, rs.getShort(10));
      Assert.assertEquals("hello", rs.getString(11));
      Assert.assertEquals(new Time(unixtime).toString(), rs.getTime(12).toString());
      Assert.assertEquals(formatter.format(new Timestamp(unixtime)),
                          formatter.format(rs.getTimestamp(13)));
      rs.close();
    }
    pstmt.close();
  }


  @Test
  public void batchInsert() throws Exception {
    Connection conn = OdpsConnectionFactory.getInstance().conn;
    Statement ddl = conn.createStatement();
    ddl.executeUpdate("drop table if exists employee_test;");
    ddl.executeUpdate("create table employee_test(id bigint, name string);");
    ddl.close();

    PreparedStatement ps = conn.prepareStatement(
        "insert into employee_test values (?, ?)");

    final int batchSize = 20000;
    int count = 0;

    for (int i = 0; i < 100000; i++) {
      ps.setInt(1, i);
      ps.setString(2, "name" + i);
      ps.addBatch();
      if(++count % batchSize == 0) {
        ps.executeBatch();
      }
    }
    ps.executeBatch(); // insert remaining records
    ps.close();

//    Statement query =  conn.createStatement();
//    ResultSet rs = query.executeQuery("select * from employee_test");
//    System.out.printf("%d columns\n", rs.getMetaData().getColumnCount());
//
//    while (rs.next()) {
//      System.out.printf("%d\t%d\t%s\n", rs.getRow(), rs.getInt(1), rs.getString(2));
//    }
//
//    rs.close();
//    query.close();
    conn.close();
  }
}
