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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;

public class OdpsResultSetMetaDataTest {

  static Statement stmt;
  static ResultSet rs;
  static ResultSetMetaData rsmd;

  @BeforeAll
  public static void setUp() throws Exception {
    stmt = TestUtils.getConnection().createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");

    Odps odps = TestUtils.getOdps();
    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UploadSession upload = tunnel.createUploadSession(odps.getDefaultProject(), "dual");
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

  @AfterAll
  public static void tearDown() throws Exception {
    rs.close();
    stmt.close();
  }

  @Test
  public void testGetColumnCount() throws Exception {
    Assertions.assertEquals(6, rsmd.getColumnCount());
  }

  @Test
  public void testGetColumnName() throws Exception {
    Assertions.assertEquals("name", rsmd.getColumnName(1));
    Assertions.assertEquals("male", rsmd.getColumnName(2));
    Assertions.assertEquals("age", rsmd.getColumnName(3));
    Assertions.assertEquals("height", rsmd.getColumnName(4));
    Assertions.assertEquals("day", rsmd.getColumnName(5));
    Assertions.assertEquals("volume", rsmd.getColumnName(6));
  }

  @Test
  public void testGetColumnType() throws Exception {
    Assertions.assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    Assertions.assertEquals(Types.BOOLEAN, rsmd.getColumnType(2));
    Assertions.assertEquals(Types.INTEGER, rsmd.getColumnType(3));
    Assertions.assertEquals(Types.DOUBLE, rsmd.getColumnType(4));
    Assertions.assertEquals(Types.TIMESTAMP, rsmd.getColumnType(5));
    Assertions.assertEquals(Types.DECIMAL, rsmd.getColumnType(6));
  }

  @Test
  public void testGetColumnTypeName() throws Exception {
    Assertions.assertEquals("STRING", rsmd.getColumnTypeName(1));
    Assertions.assertEquals("BOOLEAN", rsmd.getColumnTypeName(2));
    Assertions.assertEquals("INT", rsmd.getColumnTypeName(3));
    Assertions.assertEquals("DOUBLE", rsmd.getColumnTypeName(4));
    Assertions.assertEquals("DATETIME", rsmd.getColumnTypeName(5));
    Assertions.assertTrue(rsmd.getColumnTypeName(6).contains("DECIMAL"));
  }

  @Test
  public void testGetColumnMeta() throws Exception {
    for (int i = 0; i < rsmd.getColumnCount(); i++) {
      Assertions.assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(i + 1));
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
