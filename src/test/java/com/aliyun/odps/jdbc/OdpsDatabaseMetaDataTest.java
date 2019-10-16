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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OdpsDatabaseMetaDataTest {

  static DatabaseMetaData databaseMetaData;

  @BeforeClass
  public static void setUp() throws Exception {
    databaseMetaData = TestManager.getInstance().conn.getMetaData();
    System.out.println(databaseMetaData.getCatalogTerm());
    System.out.println(databaseMetaData.getProcedureTerm());
    System.out.println(databaseMetaData.getSchemaTerm());
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  private void printRs(ResultSet rs) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      System.out.printf("\t" + meta.getColumnName(i));
    }
    for (int i = 1; i <= meta.getColumnCount(); i++) {
      System.out.printf("\t" + meta.getColumnTypeName(i));
    }
    System.out.println();

    while(rs.next()) {
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        System.out.printf("\t" + rs.getObject(i));
      }
      System.out.println();
    }
  }

  //@Test
  public void testGetTables() throws Exception {
    {
      ResultSet rs = databaseMetaData.getTables(null, null, "%test", null);
      Assert.assertNotNull(rs);
      while (rs.next()) {
        Assert.assertTrue(rs.getString("TABLE_NAME").endsWith("test"));
        Assert.assertTrue(rs.getString("TABLE_TYPE").equals("TABLE"));

      }
      rs.close();
    }

    {
      ResultSet rs = databaseMetaData.getTables(null, null, null, new String[] {"VIEW"});
      Assert.assertNotNull(rs);
      while (rs.next()) {
        Assert.assertTrue(rs.getString("TABLE_TYPE").equals("VIEW"));
      }
      rs.close();
    }
  }

  @Test
  public void testGetFunctions() throws Exception {
    ResultSet rs = databaseMetaData.getFunctions(null, null, null);
    Assert.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetColumns() throws Exception {
    Statement stmt = TestManager.getInstance().conn.createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");
    stmt.close();

    ResultSet rs = databaseMetaData.getColumns(null, null, "dual", null);
    Assert.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetUDTs() throws Exception {
    ResultSet rs = databaseMetaData.getUDTs(null, null, null, null);
    Assert.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetPrimaryKeys() throws Exception {
    ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, null);
    Assert.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetProcedures() throws Exception {
    ResultSet rs = databaseMetaData.getProcedures(null, null, null);
    Assert.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }
}
