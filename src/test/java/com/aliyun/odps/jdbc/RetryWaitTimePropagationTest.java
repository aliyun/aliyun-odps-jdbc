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

import com.aliyun.odps.rest.RestClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The retry wait option has to reach the REST client, not just the option parser.
 *
 * <p>{@link RetryWaitTimeOptionTest} proves that {@code retryWaitTime} is read and refused
 * correctly, and every one of those cases stops at {@code ConnectionResource}. A connection is
 * where the value is either handed to the SDK or dropped, so that is what is asserted here: the
 * driver opens, and the {@link RestClient} behind it is asked what it will actually wait. Remove
 * the {@code setRetryWaitTime(...)} call in {@code OdpsConnection}'s constructor and the cases
 * below fail -- the parser tests alone do not notice.
 *
 * <p>Offline and deterministic on purpose. Every URL pins {@code timezone} and
 * {@code odpsNamespaceSchema} (plus the two modes that would otherwise ask the service), so
 * {@code connect()} returns without reading the project, and the endpoint is a port nothing
 * listens on. Nothing here sleeps, and nothing here needs credentials: the assertion is about
 * the state a connection carries into its first request, which is readable without making one.
 * The measured wall-clock effect of this option is in the PR description and the README note.
 */
class RetryWaitTimePropagationTest {

  private static final String CREDENTIALS =
      "accessId=retry_wait_time_access_id&accessKey=retry_wait_time_access_key";

  /** Pins what the driver would otherwise ask the service, so opening a connection reads nothing. */
  private static final String WITHOUT_SERVICE_READ =
      "&interactiveMode=false&useInstanceTunnel=false&timezone=Asia/Shanghai"
      + "&odpsNamespaceSchema=false";

  private static final String BASE_URL =
      "jdbc:odps:http://127.0.0.1:1/api?project=retry_wait_time_project&" + CREDENTIALS
      + WITHOUT_SERVICE_READ;

  /** @return an open connection whose transport options came from {@code extraOptions} */
  private static OdpsConnection open(String extraOptions) throws SQLException {
    Connection conn = DriverManager.getConnection(BASE_URL + extraOptions);
    assertTrue(conn instanceof OdpsConnection, conn.getClass().getName());
    return (OdpsConnection) conn;
  }

  private static RestClient restClientOf(OdpsConnection conn) {
    return conn.getOdps().getRestClient();
  }

  @Test
  void explicitRetryWaitTimeReachesTheRestClient() throws SQLException {
    try (OdpsConnection conn = open("&retryWaitTime=7")) {
      assertEquals(7, restClientOf(conn).getRetryWaitTime());
      assertEquals(7, conn.getRetryWaitTime());
    }
  }

  @Test
  void propertyKeyReachesTheRestClientToo() throws SQLException {
    Properties info = new Properties();
    info.setProperty("retry_wait_time", "3");

    try (Connection conn = DriverManager.getConnection(BASE_URL, info)) {
      assertEquals(3, restClientOf((OdpsConnection) conn).getRetryWaitTime());
    }
  }

  @Test
  void absentOptionLeavesTheSdksNotSetMarkerInPlace() throws SQLException {
    try (OdpsConnection conn = open("")) {
      // -1 is the SDK's own "not set": it then reuses the connect timeout as the interval.
      assertEquals(-1, restClientOf(conn).getRetryWaitTime());
      assertEquals(-1, conn.getRetryWaitTime());
    }
  }

  @Test
  void theRetryWaitIsDecoupledFromTheConnectTimeoutAtTheConsumer() throws SQLException {
    // This is the reported trap: a millisecond-sized connectTimeout used to size the wait too.
    try (OdpsConnection conn = open("&connectTimeout=2000&retryWaitTime=1")) {
      RestClient rest = restClientOf(conn);
      assertEquals(2000, rest.getConnectTimeout());
      assertEquals(1, rest.getRetryWaitTime());
    }
  }

  @Test
  void settingTheRetryWaitDoesNotMoveTheOtherTwoTimeouts() throws SQLException {
    try (OdpsConnection conn = open("&retryWaitTime=1")) {
      RestClient rest = restClientOf(conn);
      assertEquals(RestClient.DEFAULT_CONNECT_TIMEOUT, rest.getConnectTimeout());
      assertEquals(RestClient.DEFAULT_READ_TIMEOUT, rest.getReadTimeout());
      assertEquals(1, rest.getRetryWaitTime());
    }
  }

  @Test
  void nonPositiveUrlValuesAreIgnoredWhileTheSetterRefusesThem() throws SQLException {
    // Two documented paths, deliberately different behaviour: the URL is read by the constructor,
    // which only ever applies a positive number, so a zero or negative value cannot disable the
    // back-off -- it leaves the SDK default. The public setter refuses the same input outright.
    try (OdpsConnection conn = open("&retryWaitTime=0")) {
      assertEquals(-1, restClientOf(conn).getRetryWaitTime());
    }

    try (OdpsConnection conn = open("&retryWaitTime=-5")) {
      assertEquals(-1, restClientOf(conn).getRetryWaitTime());
    }

    try (OdpsConnection conn = open("&retryWaitTime=2")) {
      assertEquals(2, restClientOf(conn).getRetryWaitTime());

      IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
          () -> conn.setRetryWaitTime(0));
      assertTrue(zero.getMessage().contains("retry-wait-time"), zero.getMessage());
      assertThrows(IllegalArgumentException.class, () -> conn.setRetryWaitTime(-1));

      // And a value given after the connection is open reaches the same client.
      conn.setRetryWaitTime(5);
      assertEquals(5, restClientOf(conn).getRetryWaitTime());
      assertEquals(5, conn.getRetryWaitTime());
    }
  }

  @Test
  void aMistypedValueIsRefusedBeforeAnyConnectionIsOpened() {
    SQLException e = assertThrows(SQLException.class,
        () -> open("&retryWaitTime=soon"));

    assertTrue(e.getMessage().contains("retryWaitTime"), e.getMessage());
    assertTrue(e.getMessage().contains("soon"), e.getMessage());
  }
}
