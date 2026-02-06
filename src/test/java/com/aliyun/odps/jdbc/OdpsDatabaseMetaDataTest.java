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
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.jdbc.utils.TestUtils;

public class OdpsDatabaseMetaDataTest {

  static DatabaseMetaData databaseMetaData;

  @BeforeAll
  public static void setUp() throws Exception {
    databaseMetaData = TestUtils.getConnection().getMetaData();
    System.out.println(databaseMetaData.getCatalogTerm());
    System.out.println(databaseMetaData.getProcedureTerm());
    System.out.println(databaseMetaData.getSchemaTerm());
  }

  @AfterAll
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

    while (rs.next()) {
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
      Assertions.assertNotNull(rs);
      while (rs.next()) {
        Assertions.assertTrue(rs.getString("TABLE_NAME").endsWith("test"));
        Assertions.assertTrue(rs.getString("TABLE_TYPE").equals("TABLE"));

      }
      rs.close();
    }

    {
      ResultSet rs = databaseMetaData.getTables(null, null, null, new String[]{"VIEW"});
      Assertions.assertNotNull(rs);
      while (rs.next()) {
        Assertions.assertTrue(rs.getString("TABLE_TYPE").equals("VIEW"));
      }
      rs.close();
    }
  }

  @Test
  public void testGetFunctions() throws Exception {
    ResultSet rs = databaseMetaData.getFunctions(null, null, null);
    Assertions.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetColumns() throws Exception {
    Statement stmt = TestUtils.getConnection().createStatement();
    stmt.executeUpdate("drop table if exists dual;");
    stmt.executeUpdate("create table if not exists dual(id bigint);");
    stmt.close();

    ResultSet rs = databaseMetaData.getColumns(null, null, "dual", null);
    Assertions.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetUDTs() throws Exception {
    ResultSet rs = databaseMetaData.getUDTs(null, null, null, null);
    Assertions.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetPrimaryKeys() throws Exception {
    ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, null);
    Assertions.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetProcedures() throws Exception {
    ResultSet rs = databaseMetaData.getProcedures(null, null, null);
    Assertions.assertNotNull(rs);
    printRs(rs);
    rs.close();
  }

  @Test
  public void testGetCatalogs() throws Exception {
    String projectName = TestUtils.getOdps().getDefaultProject();
    try (ResultSet rs = databaseMetaData.getCatalogs()) {
      int count = 0;
      boolean includesDefaultProject = false;
      while (rs.next()) {
        String catalog = rs.getString(OdpsDatabaseMetaData.COL_NAME_TABLE_CAT);
        if (catalog.equalsIgnoreCase(projectName)) {
          includesDefaultProject = true;
        }
        count += 1;
        System.out.println(catalog);
      }
      Assertions.assertTrue(includesDefaultProject);
      Assertions.assertEquals(1, count);
    }
  }

  @Test
  public void testGetSchemas() throws Exception {
    String projectName = TestUtils.getOdps().getDefaultProject();

    // Without any filter
    try (ResultSet rs = databaseMetaData.getSchemas(null, null)) {
      int count = 0;
      boolean includesDefaultProject = false;
      while (rs.next()) {
        String schema = rs.getString(OdpsDatabaseMetaData.COL_NAME_TABLE_SCHEM);
        String catalog = rs.getString(OdpsDatabaseMetaData.COL_NAME_TABLE_CATALOG);
        // Only check equality when not in namespace schema mode
        if (!((OdpsConnection) databaseMetaData.getConnection()).isOdpsNamespaceSchema()) {
          Assertions.assertEquals(schema, catalog);
        }
        if (catalog.equalsIgnoreCase(projectName)) {
          includesDefaultProject = true;
        }
        count += 1;
      }
      Assertions.assertTrue(includesDefaultProject);
      // In namespace schema mode, there may be many schemas, so we can't assert the exact count
      if (!((OdpsConnection) databaseMetaData.getConnection()).isOdpsNamespaceSchema()) {
        Assertions.assertEquals(1, count);
      }
    }

    // Filtered by catalog name
    try (ResultSet rs = databaseMetaData.getSchemas(projectName, null)) {
      int count = 0;
      boolean includesDefaultProject = false;
      while (rs.next()) {
        String schema = rs.getString(OdpsDatabaseMetaData.COL_NAME_TABLE_SCHEM);
        String catalog = rs.getString(OdpsDatabaseMetaData.COL_NAME_TABLE_CATALOG);
        // Only check equality when not in namespace schema mode
        if (!((OdpsConnection) databaseMetaData.getConnection()).isOdpsNamespaceSchema()) {
          Assertions.assertEquals(schema, catalog);
        }
        if (catalog.equalsIgnoreCase(projectName)) {
          includesDefaultProject = true;
        }
        count += 1;
        System.out.println(String.format("%s.%s", catalog, schema));
      }
      Assertions.assertTrue(includesDefaultProject);
      // In namespace schema mode, there may be many schemas, so we can't assert the exact count
      if (!((OdpsConnection) databaseMetaData.getConnection()).isOdpsNamespaceSchema()) {
        Assertions.assertEquals(1, count);
      }
    }
  }
}