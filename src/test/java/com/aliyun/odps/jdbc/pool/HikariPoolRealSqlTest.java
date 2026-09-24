/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.jdbc.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.odps.jdbc.OdpsConnection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * The same lifecycle as {@link HikariPoolLifecycleTest}, on a real MaxCompute project: the pool is
 * not only allowed to hand connections around, the SQL on the re-used connection has to answer.
 *
 * <p>The offline suite can prove the pool notices a closed connection and stops growing its handle
 * list; only a live service can prove the replacement connection actually runs queries, that a
 * read-only pooled connection still selects, and that shutdown does not strand a session.
 *
 * <p>Skipped with a visible reason when no test identity is in the environment (see
 * {@code knowledge/jdbc-test-env-contract.md} for how the suite is given one). One query per step,
 * four steps: a live SQL round is tens of seconds on this service.
 */
@DisabledIf(value = "com.aliyun.odps.jdbc.pool.PoolHarness#noLiveService",
            disabledReason = "needs odps_endpoint / MAXCOMPUTE_PROJECT / ALIBABA_CLOUD_ACCESS_KEY_ID"
            + " / ALIBABA_CLOUD_ACCESS_KEY_SECRET in the environment")
public class HikariPoolRealSqlTest {

  private static final String ALIVE_BYPASS_WINDOW = "com.zaxxer.hikari.aliveBypassWindowMs";

  private String poolName;
  private HikariDataSource pool;

  @AfterEach
  public void closePool() {
    System.clearProperty(ALIVE_BYPASS_WINDOW);
    if (pool != null && !pool.isClosed()) {
      pool.close();
    }
  }

  private HikariDataSource openPool() {
    System.setProperty(ALIVE_BYPASS_WINDOW, "0");
    poolName = PoolHarness.nextPoolName();
    HikariConfig config = new HikariConfig();
    config.setPoolName(poolName);
    config.setJdbcUrl(PoolHarness.liveUrl());
    config.setMaximumPoolSize(2);
    config.setMinimumIdle(1);
    config.setConnectionTimeout(60_000L);
    config.setValidationTimeout(10_000L);
    config.setRegisterMbeans(false);
    return pool = new HikariDataSource(config);
  }

  /** Runs one trivial query and returns the value it read, so the assertion is about the service. */
  private static int selectOne(Connection c) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("select 1;")) {
      Assumptions.assumeTrue(rs.next(), "the service returned no row for 'select 1'");
      return rs.getInt(1);
    }
  }

  @Test
  @DisplayName("a pooled connection runs real SQL, is reused, and is replaced after it dies")
  public void poolServesRealSqlAcrossReuseAndInvalidation() throws Exception {
    openPool();
    List<OdpsConnection> opened = new ArrayList<>();

    Connection first = pool.getConnection();
    OdpsConnection firstPhysical = PoolHarness.physical(first);
    opened.add(firstPhysical);
    assertEquals(1, selectOne(first), "the first borrower got a real answer");
    first.close();

    Connection second = pool.getConnection();
    OdpsConnection secondPhysical = PoolHarness.physical(second);
    assertSame(firstPhysical, secondPhysical, "the idle connection is reused, not reopened");
    assertEquals(1, selectOne(second), "the re-used connection still talks to the service");
    second.close();

    // Kill the physical connection while the pool owns the handle: a stale session, an instance
    // that was closed by a wrapper, anything that leaves the object dead but the pool unaware.
    secondPhysical.close();
    Thread.sleep(20L);

    Connection third = pool.getConnection();
    OdpsConnection thirdPhysical = PoolHarness.physical(third);
    opened.add(thirdPhysical);
    assertNotSame(secondPhysical, thirdPhysical,
                  "the pool dropped the connection that could not validate");
    assertFalse(thirdPhysical.isClosed(), "and handed out a live one");
    assertEquals(1, selectOne(third), "real SQL runs on the replacement connection");

    pool.close();
    for (OdpsConnection physical : opened) {
      assertTrue(physical.isClosed(), "shutdown closed every connection the pool had made");
    }
    assertTrue(PoolHarness.livePoolThreadsAfter(poolName, 10_000L).isEmpty(),
               "shutdown left no thread of this pool behind");
  }

  @Test
  @DisplayName("a read-only borrower can select but cannot create a table")
  public void readOnlyIsEnforcedByTheServiceNotJustByTheDriver() throws Exception {
    openPool();

    Connection c = pool.getConnection();
    c.setReadOnly(true);
    assertEquals(1, selectOne(c), "read-only still reads");
    // The flag reaches the service as odps.sql.read.only=true, so the refusal is not ours to fake.
    // If the project ever accepts the DDL anyway, the probe table is dropped again in the same
    // statement scope and the test says which behaviour it saw instead of leaving litter.
    Statement ddl = c.createStatement();
    boolean writeRejected;
    try {
      ddl.execute("create table if not exists pool_readonly_probe (key int);");
      writeRejected = false;
    } catch (SQLException rejected) {
      writeRejected = true;
    } finally {
      ddl.close();
    }
    if (!writeRejected) {
      try (Statement cleanup = c.createStatement()) {
        cleanup.execute("drop table if exists pool_readonly_probe;");
      }
    }
    assertTrue(writeRejected,
               "read-only has to be refused by the service, not only remembered by the driver");

    c.close();
    Connection next = pool.getConnection();
    assertFalse(next.isReadOnly(), "the pool reset read-only for the next borrower");
    assertFalse(PoolHarness.physical(next).isReadOnly());
    next.close();
  }

  @Test
  @DisplayName("manual commit is refused on a live pooled connection too")
  public void autoCommitFalseIsRefusedOnTheLiveServiceAsWell() throws Exception {
    openPool();
    Connection c = pool.getConnection();
    assertThrows(SQLFeatureNotSupportedException.class, () -> c.setAutoCommit(false));
    assertEquals(1, selectOne(c), "the refusal did not damage the connection");
    c.close();
  }
}
