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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsResultSetMetaDataTest {

  static Statement stmt;
  static ResultSet rs;
  static ResultSetMetaData rsmd;

  @BeforeClass
  public static void setUp() throws Exception {
    stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");

    TableTunnel.UploadSession upload = TestManager.getInstance().tunnel.createUploadSession(
        TestManager.getInstance().odps.getDefaultProject(), "dual");
    RecordWriter writer = upload.openRecordWriter(0);
    Record r = upload.newRecord();
    r.setBigint(0, 42L);
    writer.write(r);
    writer.close();
    upload.commit(new Long[]{0L});

    String sql = "select 'yichao' name, true male, 25 age, 173.5 height, "
                 + "cast('2015-07-09 11:11:11' as datetime) day, "
                 + "cast('2.1234567890123' as decimal) volume from dual;";
    rs = stmt.executeQuery(sql);
    rsmd = rs.getMetaData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    rs.close();
    stmt.close();
  }

  @Test
  public void testGetColumnCount() throws Exception {
    Assert.assertEquals(6, rsmd.getColumnCount());
  }

  @Test
  public void testGetColumnName() throws Exception {
    Assert.assertEquals("name", rsmd.getColumnName(1));
    Assert.assertEquals("male", rsmd.getColumnName(2));
    Assert.assertEquals("age", rsmd.getColumnName(3));
    Assert.assertEquals("height", rsmd.getColumnName(4));
    Assert.assertEquals("day", rsmd.getColumnName(5));
    Assert.assertEquals("volume", rsmd.getColumnName(6));
  }

  @Test
  public void testGetColumnType() throws Exception {
    Assert.assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    Assert.assertEquals(Types.BOOLEAN, rsmd.getColumnType(2));
    Assert.assertEquals(Types.INTEGER, rsmd.getColumnType(3));
    Assert.assertEquals(Types.DOUBLE, rsmd.getColumnType(4));
    Assert.assertEquals(Types.TIMESTAMP, rsmd.getColumnType(5));
    Assert.assertEquals(Types.DECIMAL, rsmd.getColumnType(6));
  }

  @Test
  public void testGetColumnTypeName() throws Exception {
    Assert.assertEquals("STRING", rsmd.getColumnTypeName(1));
    Assert.assertEquals("BOOLEAN", rsmd.getColumnTypeName(2));
    Assert.assertEquals("INT", rsmd.getColumnTypeName(3));
    Assert.assertEquals("DOUBLE", rsmd.getColumnTypeName(4));
    Assert.assertEquals("DATETIME", rsmd.getColumnTypeName(5));
    Assert.assertEquals("DECIMAL", rsmd.getColumnTypeName(6));
  }

  @Test
  public void testGetColumnMeta() throws Exception {
    for (int i = 0; i < rsmd.getColumnCount(); i++) {
      Assert.assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(i + 1));
      int scale = rsmd.getScale(i + 1);
      int precision = rsmd.getPrecision(i + 1);
      int displaySize = rsmd.getColumnDisplaySize(i + 1);
      boolean caseSensive = rsmd.isCaseSensitive(i + 1);
      boolean signed = rsmd.isSigned(i + 1);
      System.out.printf("%d: %d %d %d %b %b\n", i + 1, scale, precision, displaySize, caseSensive,
                        signed);
    }
  }
}
