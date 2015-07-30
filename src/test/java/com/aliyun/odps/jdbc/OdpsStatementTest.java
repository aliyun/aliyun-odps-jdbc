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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsStatementTest {

  private static Connection conn = OdpsConnectionFactory.getInstance().conn;

  @BeforeClass
  public static void setUp() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(
        "create table if not exists yichao_test_table_output(id bigint);");
    stmt.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists yichao_test_table_output;");
    stmt.close();
    conn.close();
  }


  @Test
  public void testExecuteUpdate() throws Exception {
    Statement stmt = conn.createStatement();
    String sql =
        "insert into table yichao_test_table_output select * from yichao_test_table_input;";
    int updateCount = stmt.executeUpdate(sql);
    Assert.assertEquals(100 * 10000, updateCount);
  }

  /**
   * Test the scrollability of the resultset.
   * This function covers the following API of ResultSet:
   * 1. getRow()
   * 2. beforeFirst() / isBeforeFirst()
   * 4. setFetchSize() / getFetchSize()
   *
   * @throws Exception
   */
  @Test
  public void testExecuteQuery() throws Exception {
    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                          ResultSet.CONCUR_READ_ONLY);

    String sql = "select * from yichao_test_table_input;";
    ResultSet rs = stmt.executeQuery(sql);
    Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
    Assert.assertEquals(10000, stmt.getFetchSize());

    // Test performance for difference fetch size
    int[] fetchSizes = {50*10000, 20*10000, 10*10000, 5*10000, 2*10000, 10000};

    int i;
    for (int round = 0; round < fetchSizes.length; round++) {
      long start = System.currentTimeMillis();
      rs.setFetchSize(fetchSizes[round]);
      Assert.assertEquals(fetchSizes[round], rs.getFetchSize());
      rs.beforeFirst();
      Assert.assertEquals(true, rs.isBeforeFirst());
      {
        i = 0;
        while (rs.next()) {
          Assert.assertEquals(i + 1, rs.getRow());
          Assert.assertEquals(i, rs.getInt(1));
          i++;
        }
      }
      long end = System.currentTimeMillis();
      System.out.printf("step\t%d\tmillis\t%d\n", rs.getFetchSize(), end - start);
    }
  }

  @Test
  public void testExecute() throws Exception {
    Statement stmt = conn.createStatement();
    Assert.assertEquals(true, stmt.execute("select 1 id from dual;"));
    ResultSet rs = stmt.getResultSet();
    rs.next();
    Assert.assertEquals(1, rs.getInt(1));
    Assert.assertEquals(false, stmt.execute(
        "insert into table yichao_test_table_output select 1 id from dual;"));
    Assert.assertEquals(1, stmt.getUpdateCount());

    // do not check result
    Assert.assertEquals(true, stmt.execute(" select 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("\nselect 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("\t\r\nselect 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("SELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute(" SELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute(" SELECT 1 id--xixi\n from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\nSELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\n--hehehe\nSELECT 1 id from dual;"));
    Assert.assertEquals(true, stmt.execute("--abcd\n--hehehe\n\t \t select 1 id from dual;"));
  }


}
