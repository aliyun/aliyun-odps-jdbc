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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aliyun.odps.jdbc.DataUploader;
import com.aliyun.odps.jdbc.OdpsConnection;
import com.aliyun.odps.jdbc.OdpsPreparedStatement;
import com.aliyun.odps.tunnel.TunnelException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final List<Connection> borrowedHandles = new ArrayList<>();

  @BeforeEach
  public void pinPoolTiming() {
    System.setProperty(ALIVE_BYPASS_WINDOW, "0");
    poolName = PoolHarness.nextPoolName();
  }

  @AfterEach
  public void closePool() {
    System.clearProperty(ALIVE_BYPASS_WINDOW);
    for (Connection handle : borrowedHandles) {
      try {
        if (!handle.isClosed()) {
          handle.close();
        }
      } catch (SQLException ignored) {
        // A handle whose physical connection is already gone cannot be returned cleanly; giving the
        // pool its shutdown back is all this is for.
      }
    }
    borrowedHandles.clear();
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

  // ---------------------------------------------------------------------------------------------
  // commit-on-close that failed, and the cleanup that follows it
  // ---------------------------------------------------------------------------------------------

  /**
   * An uploader whose first {@code commit()} fails with a transport error and whose later ones
   * succeed, counting every attempt.
   *
   * <p>The count is the observable that matters. A prepared statement commits on close, so a close
   * that fails after the commit attempt has been made leaves behind an upload whose outcome the
   * driver never learned. Whether the service would take a second one is a question about Tunnel and
   * about {@code BasicTableUploader}/{@code AcidTableUploader}; it is not something a pooled
   * connection should get to discover by accident. The fixtures here only pin that the driver never
   * asks the question: they prove the commit is not re-attempted, not that a re-attempt would be
   * harmful on the service side.
   *
   * <p>Offline by construction. A null partition spec plus an explicit column list keeps the
   * {@link DataUploader} constructor from resolving anything from the service, which is the shape
   * {@code CallSiteSqlStateContractTest} already relies on.
   */
  private static class FirstCommitFailsUploader extends DataUploader {
    private final AtomicInteger commitAttempts = new AtomicInteger();
    private final IOException failure = new IOException(
        "the tunnel endpoint dropped the connection before the commit was acknowledged");

    FirstCommitFailsUploader(OdpsConnection conn) throws Exception {
      super(PoolHarness.PROJECT, null, "commit_on_close_table", null,
            Collections.singletonList("n_int"), conn);
    }

    @Override
    protected void setUpSession() {
    }

    @Override
    protected void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts) {
    }

    @Override
    public void commit() throws TunnelException, IOException {
      if (commitAttempts.incrementAndGet() == 1) {
        throw failure;
      }
    }
  }

  /** An uploader whose commit goes through: the positive control for the counts below. */
  private static class CountingUploader extends DataUploader {
    private final AtomicInteger commitAttempts = new AtomicInteger();

    CountingUploader(OdpsConnection conn) throws Exception {
      super(PoolHarness.PROJECT, null, "commit_on_close_table", null,
            Collections.singletonList("n_int"), conn);
    }

    @Override
    protected void setUpSession() {
    }

    @Override
    protected void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts) {
    }

    @Override
    public void commit() throws TunnelException, IOException {
      commitAttempts.incrementAndGet();
    }
  }

  /**
   * Put an uploader on a statement that never reached {@code parse()}.
   *
   * <p>Private field, written reflectively on purpose, in the same spirit as
   * {@link PoolHarness#trackedStatementCount}: the uploader is resolved while the insert executes,
   * and everything under test lives in the close path, so a real Tunnel session would add service
   * calls and prove nothing.
   */
  private static void installUploader(OdpsPreparedStatement statement, DataUploader uploader)
      throws Exception {
    Field field = OdpsPreparedStatement.class.getDeclaredField("uploader");
    field.setAccessible(true);
    field.set(statement, uploader);
  }

  /**
   * A physical driver connection borrowed from a live pool, ready to be retired.
   *
   * <p>Statements below are made on the physical connection instead of on the pool proxy for one
   * reason: HikariCP closes the statements it handed out when the handle comes back, on whichever
   * thread the pool chooses, so the cleanup being asserted would race the assertion. The walk under
   * test is the driver's own {@code OdpsConnection.close()}, which is exactly what a pool runs when
   * it discards a connection that reported 08S01.
   */
  private OdpsConnection borrowPhysicalConnection() throws Exception {
    openPool();
    Connection handle = pool.getConnection();
    borrowedHandles.add(handle);
    return PoolHarness.physical(handle);
  }

  @Test
  @DisplayName("a failed commit-on-close keeps its cause, and retiring the connection does not commit again")
  public void failedCommitOnCloseIsNotResubmittedByPoolRetirement() throws Exception {
    OdpsConnection physical = borrowPhysicalConnection();
    FirstCommitFailsUploader uploader = new FirstCommitFailsUploader(physical);
    OdpsPreparedStatement statement =
        physical.prepareStatement("insert into commit_on_close_table values (?)");
    installUploader(statement, uploader);

    SQLException reported = assertThrows(SQLException.class, statement::close);
    assertEquals("08S01", reported.getSQLState(),
                 "a commit that never got through is a broken link, and that is the state that "
                 + "tells the pool this physical connection cannot be reused");
    assertSame(uploader.failure, reported.getCause(),
               "the caller still gets the original failure as the cause");
    assertEquals(1, uploader.commitAttempts.get(), "close() attempted the commit exactly once");
    // Checked here, before any later cleanup has had a chance to fix it up: the application holds a
    // failure from close(), and the statement it called close() on has to be closed already and
    // gone from the connection's list. Leaving it open is what handed the unknown commit to the pool.
    assertTrue(statement.isClosed(),
               "a close() that reported a failure still leaves the statement closed");
    assertEquals(0, PoolHarness.trackedStatementCount(physical),
                 "and it drops the handle, which is the invariant OdpsConnection.close() reads");

    // Now the consequence of that SQLState: the pool throws the connection away, and throwing it
    // away walks the statements the application left open. On the unfixed driver the close() above
    // left this statement open *and* still tracked, so that walk commits the same unknown outcome
    // a second time -- and the second failure escapes the walk, so the pool cannot even finish
    // discarding the connection.
    physical.close();

    assertEquals(1, uploader.commitAttempts.get(),
                 "no cleanup path may re-attempt a commit whose outcome the driver never learned");
    assertTrue(physical.isClosed(), "the retired connection reaches the closed state");
  }

  @Test
  @DisplayName("closing the same statement twice after a failed commit attempts the commit once")
  public void secondExplicitCloseAfterFailedCommitDoesNotResubmit() throws Exception {
    OdpsConnection physical = borrowPhysicalConnection();
    FirstCommitFailsUploader uploader = new FirstCommitFailsUploader(physical);
    OdpsPreparedStatement statement =
        physical.prepareStatement("insert into commit_on_close_table values (?)");
    installUploader(statement, uploader);

    assertThrows(SQLException.class, statement::close);
    assertEquals(1, uploader.commitAttempts.get());
    assertTrue(statement.isClosed(), "the close() that failed still closed the statement");

    // A caller that closes again, or a layer that closes a handle the application already closed,
    // must get the JDBC answer for a closed statement: nothing happens.
    statement.close();

    assertEquals(1, uploader.commitAttempts.get(),
                 "a commit with an unknown outcome is not attempted twice by closing twice");
    physical.close();
    assertEquals(1, uploader.commitAttempts.get(),
                 "and the connection walk does not add an attempt either");
  }

  @Test
  @DisplayName("a successful commit-on-close commits once, closes the statement and drops the handle")
  public void successfulCommitOnCloseCommitsExactlyOnce() throws Exception {
    OdpsConnection physical = borrowPhysicalConnection();
    CountingUploader uploader = new CountingUploader(physical);
    OdpsPreparedStatement statement =
        physical.prepareStatement("insert into commit_on_close_table values (?)");
    installUploader(statement, uploader);

    statement.close();

    assertEquals(1, uploader.commitAttempts.get(), "commit-on-close is what the statement is for");
    assertTrue(statement.isClosed(), "the success path already got this part right");
    assertEquals(0, PoolHarness.trackedStatementCount(physical));

    physical.close();
    assertEquals(1, uploader.commitAttempts.get(),
                 "a committed upload stays committed when the connection is retired");
  }

  @Test
  @DisplayName("the implicit path -- the application never closed the statement -- also commits once")
  public void implicitCloseByTheConnectionIsNotReplayedOnARetriedClose() throws Exception {
    OdpsConnection physical = borrowPhysicalConnection();
    FirstCommitFailsUploader uploader = new FirstCommitFailsUploader(physical);
    OdpsPreparedStatement statement =
        physical.prepareStatement("insert into commit_on_close_table values (?)");
    installUploader(statement, uploader);

    // Deliberately separate from the explicit test: here nobody calls close() on the statement, so
    // the connection's own walk is the first party to attempt the commit. Reporting that failure to
    // the caller is existing behaviour and stays; what may not happen is the driver losing the
    // statement on the way out, because then every later cleanup attempt finds it again.
    assertThrows(SQLException.class, physical::close,
                 "the caller learns that the upload did not get through");
    assertEquals(1, uploader.commitAttempts.get(), "the walk attempted the commit once");
    assertTrue(statement.isClosed(),
               "the walk leaves the statement closed even though its commit failed");
    assertEquals(0, PoolHarness.trackedStatementCount(physical));

    physical.close();
    assertEquals(1, uploader.commitAttempts.get(),
                 "a caller retrying the close it was told had failed must not commit a second time");
    assertTrue(physical.isClosed(),
               "and the retry can actually finish closing the connection");
  }

  /**
   * An uploader whose commit parks until the test lets it return, so a second close() can be made
   * to arrive while the first commit is still outstanding.
   */
  private static class InFlightCommitUploader extends DataUploader {
    private final AtomicInteger commitAttempts = new AtomicInteger();
    private final CountDownLatch commitInFlight = new CountDownLatch(1);
    private final CountDownLatch commitMayReturn = new CountDownLatch(1);
    private final IOException failure =
        new IOException("the tunnel endpoint dropped the connection before the commit was acknowledged");

    InFlightCommitUploader(OdpsConnection conn) throws Exception {
      super(PoolHarness.PROJECT, null, "commit_on_close_table", null,
            Collections.singletonList("n_int"), conn);
    }

    @Override
    protected void setUpSession() {
    }

    @Override
    protected void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts) {
    }

    @Override
    public void commit() throws TunnelException, IOException {
      commitAttempts.incrementAndGet();
      commitInFlight.countDown();
      try {
        commitMayReturn.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw failure;
    }
  }

  /**
   * @return true when {@code condition} held inside the budget, instead of a bare sleep standing in for
   *     the ordering the test actually depends on
   */
  private static boolean waitUntil(Probe condition, long budgetMillis) throws Exception {
    long deadline = System.currentTimeMillis() + budgetMillis;
    while (System.currentTimeMillis() < deadline) {
      if (condition.holds()) {
        return true;
      }
      try {
        Thread.sleep(20L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.holds();
  }

  /** A condition that may itself fail: a broken read has to surface as an error, not as a timeout. */
  @FunctionalInterface
  private interface Probe {
    boolean holds() throws Exception;
  }

  @Test
  @DisplayName("a connection retired while the commit is in flight does not add a second attempt")
  public void closeRacingAnInFlightCommitCommitsOnce() throws Exception {
    OdpsConnection physical = borrowPhysicalConnection();
    InFlightCommitUploader uploader = new InFlightCommitUploader(physical);
    OdpsPreparedStatement statement =
        physical.prepareStatement("insert into commit_on_close_table values (?)");
    installUploader(statement, uploader);

    // The application closes the statement. Its commit is the call that fails, and it stays in
    // flight for the length of this test.
    final SQLException[] fromApplication = new SQLException[1];
    final CountDownLatch applicationReturned = new CountDownLatch(1);
    Thread application = new Thread(() -> {
      try {
        statement.close();
      } catch (SQLException e) {
        fromApplication[0] = e;
      } finally {
        applicationReturned.countDown();
      }
    }, "odps-commit-in-flight");
    application.start();

    assertTrue(await(uploader.commitInFlight, 5_000L), "the application never reached the commit");

    // Now the pool's own thread arrives, which is the part of this the class comment insists on: a
    // pool closes a connection from a thread the application does not control, at a moment the
    // application did not choose. The statement is open at this instant by construction, because
    // the driver marks it closed only after the commit returns.
    final CountDownLatch poolReturned = new CountDownLatch(1);
    Thread retirement = new Thread(() -> {
      try {
        physical.close();
      } catch (SQLException ignored) {
        // What the pool does with a failure here is HikariCP's business; the claim is about the
        // number of commits, which is counted below.
      } finally {
        poolReturned.countDown();
      }
    }, "odps-pool-retirement");
    retirement.start();

    // Wait until the second close() has demonstrably reached the statement -- either by finishing
    // the close (the guarded driver never touches the commit again) or by entering a second commit
    // (the unguarded one does) -- and only then let the first commit fail.
    boolean reachedTheStatement = waitUntil(
        () -> statement.isClosed() || uploader.commitAttempts.get() > 1, 5_000L);
    uploader.commitMayReturn.countDown();
    assertTrue(await(applicationReturned, 5_000L), "the application thread never came back");
    assertTrue(await(poolReturned, 5_000L), "the pool thread never came back");
    assertTrue(reachedTheStatement,
               "the retiring connection never reached the still-open statement: the interleaving "
               + "this test is about did not happen");

    assertEquals(1, uploader.commitAttempts.get(),
                 "while a commit is in flight, no other thread may start one for the same upload");
    assertNotNull(fromApplication[0], "the application still learns that its commit failed");
    assertEquals("08S01", fromApplication[0].getSQLState(),
                 "and it learns it as the link failure that makes the pool retire the connection");
    assertSame(uploader.failure, fromApplication[0].getCause());
    assertTrue(statement.isClosed(), "the statement ends up closed either way");
  }

  private static boolean await(CountDownLatch latch, long millis) throws InterruptedException {
    return latch.await(millis, TimeUnit.MILLISECONDS);
  }
}
