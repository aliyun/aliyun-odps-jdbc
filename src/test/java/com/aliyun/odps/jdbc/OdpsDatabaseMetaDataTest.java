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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class OdpsDatabaseMetaDataTest {

  static DatabaseMetaData databaseMetaData;

  @BeforeClass
  public static void setUp() throws Exception {
    databaseMetaData = OdpsConnectionFactory.getInstance().conn.getMetaData();
    System.out.println(databaseMetaData.getCatalogTerm());
    System.out.println(databaseMetaData.getProcedureTerm());
    System.out.println(databaseMetaData.getSchemaTerm());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    OdpsConnectionFactory.getInstance().conn.close();
  }

  @Test
  public void testGetTables() throws Exception {
    ResultSet rs = databaseMetaData.getTables(null, null, null, null);
    Assert.assertNotNull(rs);
    rs.close();
  }

  @Test
  public void testGetFunctions() throws Exception {
    ResultSet rs = databaseMetaData.getFunctions(null, null, null);
    Assert.assertNotNull(rs);
    rs.close();
  }

  @Test
  public void testGetColumns() throws Exception {
    ResultSet rs = databaseMetaData.getColumns(null, null, "zhemin_test", null);
    Assert.assertNotNull(rs);
    rs.close();
  }

  @Test
  public void testGetUDTs() throws Exception {
    ResultSet rs = databaseMetaData.getUDTs(null, null, null, null);
    Assert.assertNotNull(rs);
    rs.close();
  }

  @Test
  public void testGetPrimaryKeys() throws Exception {
    ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, null);
    Assert.assertNotNull(rs);
    rs.close();
  }

  @Test
  public void testGetProcedures() throws Exception {
    ResultSet rs = databaseMetaData.getProcedures(null, null, null);
    Assert.assertNotNull(rs);
    rs.close();
  }
}
