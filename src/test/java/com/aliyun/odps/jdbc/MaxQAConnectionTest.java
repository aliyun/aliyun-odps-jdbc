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
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.sqa.ExecuteMode;

/**
 * Tests for MaxQA (MaxCompute Query Acceleration) connection support.
 */
public class MaxQAConnectionTest {

  /**
   * Test that connection with default quota does not enable MaxQA.
   */
  @Test
  public void testDefaultQuotaDoesNotEnableMaxQA() throws Exception {
    Connection conn = TestUtils.getConnection();
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      // Default quota should not enable MaxQA
      Assertions.assertFalse(odpsConn.isEnableMaxQA(), 
          "Default quota should not enable MaxQA");
      Assertions.assertNotEquals(ExecuteMode.INTERACTIVE_V2, odpsConn.getInteractiveMode(),
          "Default quota should not use INTERACTIVE_V2 mode");
    } finally {
      conn.close();
    }
  }

  /**
   * Test that connection with explicit quota name can enable MaxQA if quota supports it.
   * Note: This test requires a valid MaxQA quota to be configured in the environment.
   */
  @Test
  public void testExplicitQuotaMayEnableMaxQA() throws Exception {
    String quotaName = System.getenv("MAXCOMPUTE_QUOTA_NAME");
    if (quotaName == null || quotaName.isEmpty()) {
      // Skip test if no quota name is configured
      return;
    }
    
    Connection conn = TestUtils.getConnection(
        Collections.singletonMap("quotaName", quotaName));
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      // If quota is "default", MaxQA should not be enabled
      if ("default".equalsIgnoreCase(quotaName)) {
        Assertions.assertFalse(odpsConn.isEnableMaxQA(),
            "Default quota should not enable MaxQA");
      }
      // Otherwise, MaxQA state depends on whether the quota supports it
    } finally {
      conn.close();
    }
  }

  /**
   * Test that SQL execution works correctly with MaxQA connection.
   */
  @Test
  public void testQueryExecutionWithMaxQA() throws Exception {
    String quotaName = System.getenv("MAXCOMPUTE_QUOTA_NAME");
    Connection conn;
    if (quotaName != null && !quotaName.isEmpty()) {
      conn = TestUtils.getConnection(
          Collections.singletonMap("quotaName", quotaName));
    } else {
      conn = TestUtils.getConnection();
    }
    
    try (Statement stmt = conn.createStatement()) {
      // Test simple query
      ResultSet rs = stmt.executeQuery("SELECT 1 as test_col");
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(1, rs.getInt(1));
      rs.close();
      
      // Test query with multiple columns
      rs = stmt.executeQuery("SELECT 1 as col1, 'test' as col2, 3.14 as col3");
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(1, rs.getInt("col1"));
      Assertions.assertEquals("test", rs.getString("col2"));
      Assertions.assertEquals(3.14, rs.getDouble("col3"), 0.01);
      rs.close();
    } finally {
      conn.close();
    }
  }

  /**
   * Test that SET clause for quota is properly handled (no longer dynamically switches MaxQA).
   */
  @Test
  public void testSetQuotaDoesNotDynamicallySwitchMaxQA() throws Exception {
    Connection conn = TestUtils.getConnection();
    try (Statement stmt = conn.createStatement()) {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      boolean initialMaxQAState = odpsConn.isEnableMaxQA();
      
      // SET quota should be processed but not dynamically switch MaxQA mode
      stmt.execute("SET odps.task.wlm.quota = some_quota_name");
      
      // MaxQA state should remain unchanged after SET
      Assertions.assertEquals(initialMaxQAState, odpsConn.isEnableMaxQA(),
          "SET odps.task.wlm.quota should not dynamically change MaxQA state");
    } finally {
      conn.close();
    }
  }

  /**
   * Test connection mode consistency.
   */
  @Test
  public void testConnectionModeConsistency() throws Exception {
    Connection conn = TestUtils.getConnection();
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      ExecuteMode mode = odpsConn.getInteractiveMode();
      
      // Mode should be consistent
      Assertions.assertNotNull(mode);
      
      // If MaxQA is enabled, mode should be INTERACTIVE_V2
      if (odpsConn.isEnableMaxQA()) {
        Assertions.assertEquals(ExecuteMode.INTERACTIVE_V2, mode,
            "MaxQA enabled connection should use INTERACTIVE_V2 mode");
      }
    } finally {
      conn.close();
    }
  }
}
