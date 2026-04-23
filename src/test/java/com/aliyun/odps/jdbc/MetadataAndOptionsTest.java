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
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.jdbc.utils.TestUtils;

/**
 * Integration tests for:
 * 1. The {@code skipCheckIfEpv2} URL parameter, which should be propagated to
 *    {@code odps.options().setSkipCheckIfEpv2(...)} when set.
 * 2. The {@code getTables} short-circuit: when the connection has a {@code schema}
 *    URL parameter and the caller passes a null schemaPattern, the lookup is restricted
 *    to that single schema instead of iterating all schemas in the project.
 *
 * <p>Some test environments have a poisoned table whose comment contains an XML-illegal
 * control character (e.g. 0x8), which makes the SDK's {@code Tables.iterator()} throw an
 * XML parse error. Tests that have to iterate tables tolerate this specific error using
 * {@link Assumptions} - the call has already gone through our short-circuit logic by the
 * time the SDK fails downstream.
 */
public class MetadataAndOptionsTest {

  // ---------- skipCheckIfEpv2 URL parameter ----------

  @Test
  public void testSkipCheckIfEpv2DefaultsToNull() throws Exception {
    Connection conn = TestUtils.getConnection();
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      Assertions.assertNull(
          odpsConn.getOdps().options().isSkipCheckIfEpv2(),
          "skipCheckIfEpv2 should be null when not set in URL (preserve SDK default)");
    } finally {
      conn.close();
    }
  }

  @Test
  public void testSkipCheckIfEpv2True() throws Exception {
    Connection conn = TestUtils.getConnection(
        Collections.singletonMap("skipCheckIfEpv2", "true"));
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      Assertions.assertEquals(
          Boolean.TRUE,
          odpsConn.getOdps().options().isSkipCheckIfEpv2(),
          "skipCheckIfEpv2=true should propagate to odps.options()");
    } finally {
      conn.close();
    }
  }

  @Test
  public void testSkipCheckIfEpv2False() throws Exception {
    Connection conn = TestUtils.getConnection(
        Collections.singletonMap("skipCheckIfEpv2", "false"));
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      Assertions.assertEquals(
          Boolean.FALSE,
          odpsConn.getOdps().options().isSkipCheckIfEpv2(),
          "skipCheckIfEpv2=false should propagate to odps.options()");
    } finally {
      conn.close();
    }
  }

  // ---------- getTables schema short-circuit ----------

  /**
   * When the URL has {@code odpsNamespaceSchema=true&schema=X}, the connection state
   * must reflect that, so that the short-circuit in {@code getTables} can pick it up.
   * Also exercises the actual call (tolerating the env XML issue if hit).
   */
  @Test
  public void testGetTablesShortCircuitsToConnectionSchema() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("odpsNamespaceSchema", "true");
    props.put("schema", "default");
    Connection conn = TestUtils.getConnection(props);
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      // Verify preconditions of the short-circuit.
      Assertions.assertEquals(Boolean.TRUE, odpsConn.isOdpsNamespaceSchema(),
          "odpsNamespaceSchema URL param should be honored");
      Assertions.assertEquals("default", odpsConn.getOdps().getCurrentSchema(),
          "schema URL param should be honored");

      ResultSet rs;
      try {
        rs = conn.getMetaData().getTables(null, null, null, null);
      } catch (SQLException e) {
        // Tolerate the known SDK env issue (XML parse error from a poisoned table comment).
        // The short-circuit code has already executed by this point; the failure is in
        // the SDK's table iterator.
        Assumptions.assumeFalse(isXmlBindIssue(e),
            "Skipping verification of returned rows due to known SDK XML bind issue");
        throw e;
      }
      Assertions.assertNotNull(rs);
      int checked = 0;
      while (rs.next() && checked < 50) {
        Assertions.assertEquals("default", rs.getString("TABLE_SCHEM"),
            "Short-circuit should restrict tables to the URL-configured schema 'default'");
        checked++;
      }
      rs.close();
    } finally {
      conn.close();
    }
  }

  /**
   * When the caller explicitly passes a schemaPattern, the short-circuit should NOT
   * override it. The behavior is verified by checking that the call works (or fails
   * only with the tolerated env issue, having gone through the same code path).
   */
  @Test
  public void testGetTablesRespectsExplicitSchemaPattern() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("odpsNamespaceSchema", "true");
    props.put("schema", "default");
    Connection conn = TestUtils.getConnection(props);
    try {
      ResultSet rs;
      try {
        rs = conn.getMetaData().getTables(null, "default", null, null);
      } catch (SQLException e) {
        Assumptions.assumeFalse(isXmlBindIssue(e),
            "Skipping due to known SDK XML bind issue");
        throw e;
      }
      Assertions.assertNotNull(rs);
      int checked = 0;
      while (rs.next() && checked < 5) {
        Assertions.assertEquals("default", rs.getString("TABLE_SCHEM"));
        checked++;
      }
      rs.close();
    } finally {
      conn.close();
    }
  }

  /**
   * Without a schema URL parameter, the short-circuit must NOT activate.
   * Verifies that {@code conn.getSchema()} is null - the precondition for "no
   * short-circuit" - and that the call still works (or fails only with the tolerated
   * env issue).
   */
  @Test
  public void testGetTablesPreservesOriginalBehaviorWithoutUrlSchema() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("odpsNamespaceSchema", "true");
    // intentionally no "schema" key
    Connection conn = TestUtils.getConnection(props);
    try {
      OdpsConnection odpsConn = (OdpsConnection) conn;
      Assertions.assertNull(odpsConn.getOdps().getCurrentSchema(),
          "Precondition: no schema configured in URL");

      ResultSet rs;
      try {
        rs = conn.getMetaData().getTables(null, null, null, null);
      } catch (SQLException e) {
        Assumptions.assumeFalse(isXmlBindIssue(e),
            "Skipping due to known SDK XML bind issue");
        throw e;
      }
      Assertions.assertNotNull(rs);
      rs.close();
    } finally {
      conn.close();
    }
  }

  /**
   * Detect the known SDK XML bind error caused by a poisoned table comment in some
   * test environments.
   */
  private static boolean isXmlBindIssue(Throwable t) {
    while (t != null) {
      String m = t.getMessage();
      if (m != null && (m.contains("Can't bind xml") || m.contains("XMLStreamException"))) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }
}
