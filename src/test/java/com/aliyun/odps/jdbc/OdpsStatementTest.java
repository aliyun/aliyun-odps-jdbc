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
import java.sql.Statement;

import junit.framework.TestCase;

public class OdpsStatementTest extends TestCase {

  protected Statement stmt;

  protected void setUp() throws Exception {
    stmt = OdpsConnectionFactory.getInstance().conn.createStatement();
    stmt.executeUpdate(
        "create table if not exists yichao_test_table_output(id bigint);");
  }

  protected void tearDown() throws Exception {
    stmt.executeUpdate("drop table if exists yichao_test_table_output;");
    stmt.close();
  }

  public void testExecute() throws Exception {
    assertEquals(true, stmt.execute("select 1 id from dual;"));
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));
    assertEquals(false, stmt.execute(
        "insert into table yichao_test_table_output select 1 id from dual;"));
    assertEquals(1, stmt.getUpdateCount());

    // do not check result
    assertEquals(true, stmt.execute(" select 1 id from dual;"));
    assertEquals(true, stmt.execute("\nselect 1 id from dual;"));
    assertEquals(true, stmt.execute("\t\r\nselect 1 id from dual;"));
    assertEquals(true, stmt.execute("SELECT 1 id from dual;"));
    assertEquals(true, stmt.execute(" SELECT 1 id from dual;"));
    assertEquals(true, stmt.execute("--abcd\nSELECT 1 id from dual;"));
    assertEquals(true, stmt.execute("--abcd\n--hehehe\nSELECT 1 id from dual;"));
    assertEquals(true, stmt.execute("--abcd\n--hehehe\n\t \t select 1 id from dual;"));
  }

  public void testExecuteUpdate() throws Exception {
    String sql =
        "insert into table yichao_test_table_output select * from yichao_test_table_input;";
    int updateCount = stmt.executeUpdate(sql);
    assertEquals(100*10000, updateCount);
  }

  public void testExecuteQuery() throws Exception {
    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);

    int i = 0;
    while (rs.next()) {
      assertEquals(i, rs.getInt(1));
      i++;
    }
  }
}
