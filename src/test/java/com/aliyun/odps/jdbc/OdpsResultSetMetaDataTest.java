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

import junit.framework.TestCase;

public class OdpsResultSetMetaDataTest extends TestCase {

  protected Statement stmt;
  protected ResultSet rs;
  protected ResultSetMetaData rsmd;

  protected void setUp() throws Exception {
    stmt = OdpsConnectionFactory.getInstance().conn.createStatement();
    String sql = "select 'yichao' name, true male, 25 age, 173.5 height, "
                 + "cast('2015-07-09 11:11:11' as datetime) day, "
                 + "cast('2.1234567890123' as decimal) volume from dual;";
    rs = stmt.executeQuery(sql);
    rsmd = rs.getMetaData();
  }

  protected void tearDown() throws Exception {
    stmt.close();
    rs.close();
  }

  public void testGetColumnCount() throws Exception {
    assertEquals(6, rsmd.getColumnCount());
  }

  public void testGetColumnName() throws Exception {
    assertEquals("name", rsmd.getColumnName(1));
    assertEquals("male", rsmd.getColumnName(2));
    assertEquals("age", rsmd.getColumnName(3));
    assertEquals("height", rsmd.getColumnName(4));
    assertEquals("day", rsmd.getColumnName(5));
    assertEquals("volume", rsmd.getColumnName(6));
  }

  public void testGetColumnType() throws Exception {
    assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
    assertEquals(Types.BOOLEAN, rsmd.getColumnType(2));
    assertEquals(Types.BIGINT, rsmd.getColumnType(3));
    assertEquals(Types.DOUBLE, rsmd.getColumnType(4));
    assertEquals(Types.DATE, rsmd.getColumnType(5));
    assertEquals(Types.DECIMAL, rsmd.getColumnType(6));
  }
}
