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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsPreparedStatementTest {

  static PreparedStatement pstmt;
  static long unixtime;
  static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


  @BeforeClass
  public static void setUp() throws Exception {
    pstmt = OdpsConnectionFactory.getInstance().conn.prepareStatement(
        "select ? c1, ? c2, ? c3, ? c4, ? c5, ? c6, "
        + "? c7, ? c8, ? c9, ? c10, ? c11, ? c12, ? c13 from dual;");
    unixtime = new java.util.Date().getTime();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    pstmt.close();
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testSetAll() throws Exception {
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
  }
}
