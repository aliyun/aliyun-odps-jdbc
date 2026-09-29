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
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * HikariCP consumer regression: what a pool does to a driver connection over its life, and what
 * the driver owes the pool back.
 *
 * <p>The driver's own connection tests cannot cover this. A pool is a second client of the same
 * object: it validates a connection before handing it to the next borrower, resets state when the
 * previous one returns it, and closes it when it expires. Every one of those calls reaches driver
 * code from a thread the application does not control, at a time the application did not choose.
 *
 * <p>Nothing here needs cloud credentials. {@link PoolHarness#deadEndpointUrl()} returns a URL
 * whose endpoint is a port nothing listens on, and a MaxCompute connection in the default offline
 * mode is constructible against it: the driver holds no server session, the REST layer signs each
 * request separately, and the harness turns off the two fetches the constructor would otherwise
 * make (project properties, tunnel route). Executing SQL on such a connection fails, which is the
 * injected fault.
 *
 * <p>HikariCP 4.0.3 is the newest release that runs on the JDK 8 bytecode this driver targets, so
 * it is also the newest pool a JDK 8 consumer of odps-jdbc can put on the classpath.
 */
public class HikariPoolLifecycleTest {

  /**
   * HikariCP skips validation for a connection borrowed within this window of its last use. Zero
   * makes every borrow take the {@code isValid()} path, which is the call under test.
   */
  private static final String ALIVE_BYPASS_WINDOW = "com.zaxxer.hikari.aliveBypassWindowMs";

  private String poolName;
  private HikariDataSource pool;

  @BeforeEach
  public void pinPoolTiming() {
    System.setProperty(ALIVE_BYPASS_WINDOW, "0");
    poolName = PoolHarness.nextPoolName();
  }

  @AfterEach
  public void closePool() {
    System.clearProperty(ALIVE_BYPASS_WINDOW);
    if (pool != null && !pool.isClosed()) {
      pool.close();
    }
  }

  private HikariConfig poolConfig() throws Exception {
    HikariConfig config = new HikariConfig();
    config.setPoolName(poolName);
    config.setDriverClassName("com.aliyun.odps.jdbc.OdpsDriver");
    config.setJdbcUrl(PoolHarness.deadEndpointUrl());
    config.setMaximumPoolSize(2);
    config.setMinimumIdle(0);
    config.setConnectionTimeout(4_000L);
    config.setValidationTimeout(5_000L);
    config.setRegisterMbeans(false);
    return config;
  }

  private HikariDataSource openPool() throws Exception {
    return pool = new HikariDataSource(poolConfig());
  }

  @Test
  @DisplayName("borrow, return, borrow again hands out the same physical connection")
  public void borrowReturnBorrowReusesOnePhysicalConnection() throws Exception {
    openPool();

    Connection first = pool.getConnection();
    OdpsConnection firstPhysical = PoolHarness.physical(first);
    first.close();

    Connection second = pool.getConnection();
    OdpsConnection secondPhysical = PoolHarness.physical(second);

    assertSame(firstPhysical, secondPhysical,
               "an idle connection has to be reused instead of opening a second one");
    assertFalse(secondPhysical.isClosed(), "the reused connection is still open");
    second.close();
  }

  @Test
  @DisplayName("returning a connection twice neither closes it nor loses it")
  public void doubleCloseReturnsTheConnectionOnce() throws Exception {
    openPool();

    Connection borrowed = pool.getConnection();
    OdpsConnection physical = PoolHarness.physical(borrowed);
    borrowed.close();
    borrowed.close(); // JDBC: close() on an already-closed pooled handle is a no-op

    assertFalse(physical.isClosed(),
                "closing the pooled handle must not close the physical connection");

    Connection again = pool.getConnection();
    assertSame(physical, PoolHarness.physical(again));
    again.close();
  }

  @Test
  @DisplayName("a connection closed behind the pool's back is not given to the next borrower")
  public void physicallyClosedConnectionIsNotReused() throws Exception {
    openPool();

    Connection borrowed = pool.getConnection();
    OdpsConnection killed = PoolHarness.physical(borrowed);
    // What a leak detector, another layer that unwrapped the driver connection, or a close() that
    // reached the physical object directly amounts to: the connection is gone while the pool still
    // believes it holds a usable one.
    killed.close();
    borrowed.close();
    // HikariCP only revalidates a connection that has been idle for longer than its bypass window
    // (pinned to 0 ms above), so give the clock a tick to notice the return.
    Thread.sleep(20L);

    Connection next = pool.getConnection();
    OdpsConnection nextPhysical = PoolHarness.physical(next);

    assertTrue(killed.isClosed(), "the killed connection stayed closed");
    assertNotSame(killed, nextPhysical,
                  "the pool must not validate a closed connection as alive and re-issue it");
    assertFalse(nextPhysical.isClosed(), "the borrower got a connection it can use");
    next.createStatement().close();
    next.close();
  }

  @Test
  @DisplayName("isValid reports the closed state and refuses a negative timeout")
  public void isValidFollowsTheJdbcContract() throws Exception {
    openPool();
    Connection handle = pool.getConnection();
    OdpsConnection physical = PoolHarness.physical(handle);
    handle.close();

    assertTrue(physical.isValid(0), "an open connection is valid with no timeout");
    assertTrue(physical.isValid(1), "an open connection is valid with a timeout");

    physical.close();
    assertFalse(physical.isValid(1),
                "a closed connection is not valid -- this answer is what lets a pool replace it");
    physical.close();
    assertTrue(physical.isClosed(), "close() of a closed connection stays a no-op");

    SQLException negative = assertThrows(SQLException.class, () -> physical.isValid(-1));
    assertTrue(negative.getMessage().contains("negative"), negative.getMessage());
  }

  @Test
  @DisplayName("a pool configured for manual commits cannot start, and says why")
  public void autoCommitFalseIsRejectedAtPoolStart() throws Exception {
    HikariConfig config = poolConfig();
    // The configuration a transactional RDBMS pool normally uses. MaxCompute has no transaction to
    // close, so the driver says so and the pool refuses to open rather than pretending.
    config.setAutoCommit(false);

    Exception failure = assertThrows(Exception.class, () -> new HikariDataSource(config));
    Throwable root = failure;
    while (root.getCause() != null && !(root instanceof SQLFeatureNotSupportedException)) {
      root = root.getCause();
    }
    assertTrue(root instanceof SQLFeatureNotSupportedException,
               "expected an unsupported-feature failure, got " + root);

    Connection handle = openPool().getConnection();
    try {
      assertThrows(SQLFeatureNotSupportedException.class, () -> handle.setAutoCommit(false));
      assertTrue(handle.getAutoCommit(), "the driver is always in autocommit");
      assertThrows(SQLFeatureNotSupportedException.class, handle::commit);
      assertThrows(SQLFeatureNotSupportedException.class, handle::rollback);
    } finally {
      handle.close();
    }
  }

  @Test
  @DisplayName("read-only set by one borrower is reset before the next one gets the connection")
  public void readOnlyIsResetByThePoolOnReturn() throws Exception {
    openPool();

    Connection first = pool.getConnection();
    first.setReadOnly(true);
    assertTrue(first.isReadOnly(), "the driver keeps the read-only flag on the pooled handle ...");
    assertTrue(PoolHarness.physical(first).isReadOnly(),
               "... and on the physical connection, where it becomes odps.sql.read.only");
    first.close();

    Connection second = pool.getConnection();
    assertFalse(second.isReadOnly(), "the pool resets read-only before handing the connection on");
    assertFalse(PoolHarness.physical(second).isReadOnly(),
                "the reset has to reach the physical connection, not just the proxy");
    second.close();
  }

  @Test
  @DisplayName("a catalog changed by one borrower survives unless the pool pins one")
  public void catalogIsOnlyResetWhenThePoolConfiguresOne() throws Exception {
    String otherProject = "pool_lifecycle_other_project";

    openPool(); // no pool-level catalog: HikariCP has nothing to reset to
    Connection borrower = pool.getConnection();
    borrower.setCatalog(otherProject);
    assertEquals(otherProject, borrower.getCatalog());
    borrower.close();

    Connection nextBorrower = pool.getConnection();
    assertEquals(otherProject, PoolHarness.physical(nextBorrower).getCatalog(),
                 "documented pool behaviour: with no configured catalog the project the previous "
                 + "borrower selected is the project the next borrower silently gets");
    nextBorrower.close();
    pool.close();

    HikariConfig pinned = poolConfig();
    pinned.setCatalog(PoolHarness.PROJECT);
    openPool2(pinned);
    Connection changed = pool.getConnection();
    changed.setCatalog(otherProject);
    changed.close();

    Connection afterReset = pool.getConnection();
    assertEquals(PoolHarness.PROJECT, PoolHarness.physical(afterReset).getCatalog(),
                 "a pool that pins its catalog gets the project back on every borrow: this is the "
                 + "configuration a pooled application has to use if it switches project at all");
    afterReset.close();
  }

  private HikariDataSource openPool2(HikariConfig config) {
    return pool = new HikariDataSource(config);
  }

  @Test
  @DisplayName("a two-tier project drops setSchema without an error; getSchema is the signal")
  public void schemaIsNotSupportedOnTwoTierProjects() throws Exception {
    openPool();
    Connection c = pool.getConnection();
    // The harness URL asks for a two-tier project (odpsNamespaceSchema=false).
    assertNull(c.getSchema(), "a two-tier project reports no schema");
    c.setSchema("ignored_by_a_two_tier_project");
    assertNull(c.getSchema(),
               "setSchema() is dropped rather than rejected on a two-tier project. Consumers that "
               + "need three-tier naming have to set odpsNamespaceSchema=true; the driver keeps "
               + "accepting the call so that a pool or ORM that always calls setSchema() does not "
               + "start failing on upgrade.");
    c.close();
  }

  @Test
  @DisplayName("closed statements stop occupying the connection's handle list")
  public void closedStatementsAreNotRetained() throws Exception {
    openPool();
    Connection handle = pool.getConnection();
    OdpsConnection physical = PoolHarness.physical(handle);

    for (int i = 0; i < 200; i++) {
      Statement statement = handle.createStatement();
      statement.close();
    }
    assertEquals(0, PoolHarness.trackedStatementCount(physical),
                 "a connection that lives for hours in a pool must not accumulate one handle per "
                 + "statement ever created on it");

    Statement open1 = handle.createStatement();
    Statement open2 = handle.prepareStatement("select 1");
    assertEquals(2, PoolHarness.trackedStatementCount(physical),
                 "open statements stay tracked so that closing the connection can still clean up");
    open1.close();
    assertEquals(1, PoolHarness.trackedStatementCount(physical));
    open2.close();
    assertEquals(0, PoolHarness.trackedStatementCount(physical));
    handle.close();
  }

  @Test
  @DisplayName("a statement left open is closed when the connection comes back to the pool")
  public void statementsAreClosedWhenTheConnectionIsReturned() throws Exception {
    openPool();
    Connection handle = pool.getConnection();
    OdpsConnection physical = PoolHarness.physical(handle);
    handle.createStatement();
    assertEquals(1, PoolHarness.trackedStatementCount(physical));

    handle.close(); // HikariCP closes statements made through the handle

    assertEquals(0, PoolHarness.trackedStatementCount(physical),
                 "returning a connection must not leave a statement tracked open");
    assertFalse(physical.isClosed(), "returning a connection does not close it");
  }

  @Test
  @DisplayName("pool shutdown closes the physical connections and leaves no pool threads")
  public void shutdownReclaimsConnectionsAndThreads() throws Exception {
    List<OdpsConnection> opened = new ArrayList<>();
    openPool();
    for (int i = 0; i < 4; i++) {
      Connection handle = pool.getConnection();
      opened.add(PoolHarness.physical(handle));
      handle.createStatement().close();
      handle.close();
    }
    assertTrue(pool.isRunning(), "the pool is up before shutdown");

    pool.close();

    for (OdpsConnection physical : opened) {
      assertTrue(physical.isClosed(), "every physical connection the pool made is closed");
    }
    assertTrue(PoolHarness.livePoolThreadsAfter(poolName, 5_000L).isEmpty(),
               "no thread named after the pool survives shutdown, so a JVM that drops the last "
               + "reference to the pool can still exit");
  }

  @Test
  @DisplayName("a connection still borrowed at shutdown is not left open behind a dead pool")
  public void shutdownWithAnOpenBorrowReclaimsTheConnection() throws Exception {
    openPool();
    Connection held = pool.getConnection();
    OdpsConnection physical = PoolHarness.physical(held);

    pool.close();
    // The driver has no abort(): shutdown can only mark the entry evicted, and the physical close
    // happens when the borrower gives the handle back, or when the pool can close it directly.
    held.close();

    assertTrue(physical.isClosed(),
               "the borrowed connection is closed on the way out, not kept by a pool that is gone");
    assertThrows(SQLException.class, held::createStatement,
                 "the handle cannot be used again after shutdown");
  }

  @Test
  @DisplayName("a leaked connection is not cancelled by the pool: the driver has no abort()")
  public void leakedConnectionIsHeldOpenAndBorrowTimesOut() throws Exception {
    openPool();
    List<Connection> leaked = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      leaked.add(pool.getConnection()); // never returned
    }

    SQLTransientConnectionException timeout =
        assertThrows(SQLTransientConnectionException.class, pool::getConnection);
    assertTrue(timeout.getMessage().contains("timed out"), timeout.getMessage());
    for (Connection handle : leaked) {
      assertFalse(PoolHarness.physical(handle).isClosed(),
                  "nothing closed the leaked connections: abort() is unsupported, so the pool can "
                  + "only report the exhaustion; leakDetectionThreshold is the usable signal");
    }
    for (Connection handle : leaked) {
      handle.close();
    }
  }

  @Test
  @DisplayName("unsupported connection properties fail as SQLFeatureNotSupportedException")
  public void unsupportedPropertiesFailLoudly() throws Exception {
    openPool();
    Connection c = pool.getConnection();

    assertThrows(SQLFeatureNotSupportedException.class,
                 () -> c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation(),
                 "the one answer the driver gives about transactions is 'none'");
    assertThrows(SQLFeatureNotSupportedException.class,
                 () -> c.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, c.getHoldability());
    assertThrows(SQLFeatureNotSupportedException.class,
                 () -> c.setNetworkTimeout(runnable -> runnable.run(), 1_000));
    assertThrows(SQLFeatureNotSupportedException.class, c::getNetworkTimeout);
    assertThrows(SQLFeatureNotSupportedException.class,
                 () -> c.abort(runnable -> runnable.run()));
    assertThrows(SQLFeatureNotSupportedException.class, () -> c.nativeSQL("select 1"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> c.prepareCall("{call x}"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> c.setTypeMap(null));
    assertThrows(SQLFeatureNotSupportedException.class, c::createClob);
    assertThrows(SQLFeatureNotSupportedException.class, () -> c.prepareStatement("select 1",
                                                                                 Statement.RETURN_GENERATED_KEYS));
    c.close();
  }
}
