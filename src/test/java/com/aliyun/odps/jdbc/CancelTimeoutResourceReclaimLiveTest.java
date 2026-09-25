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
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.jdbc.utils.TestUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledIf;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The same cancel / close races against a real MaxCompute project, so the offline mock tests are
 * not the only thing standing behind the claim:
 *
 * <ul>
 *   <li>a running query cancelled through the driver must reach a terminal state;</li>
 *   <li>closing a statement under a live multi-threaded download must give the reading thread
 *       back and let its thread pools disappear;</li>
 *   <li>interleaved and repeated closes of real result set / statement / connection objects stay
 *       quiet, and reuse after close reports SQLException.</li>
 * </ul>
 *
 * <p>Waits are deadlines around a condition, never fixed sleeps, so a pass does not depend on
 * how loaded the cluster happens to be. Nothing below asserts how many records of a partially
 * downloaded result reached the client: the driver makes no exactly-once promise about a stream
 * that someone cancelled halfway.
 */
@Timeout(value = 20, unit = TimeUnit.MINUTES)
@DisabledIf(
    value = "com.aliyun.odps.jdbc.CancelTimeoutResourceReclaimLiveTest#withoutTestIdentity",
    disabledReason = "needs the component test identity: odps_endpoint / MAXCOMPUTE_PROJECT / "
                     + "ALIBABA_CLOUD_ACCESS_KEY_ID")
public class CancelTimeoutResourceReclaimLiveTest {

  private static final String TABLE = "tony_cancel_close_probe";
  private static final int ROWS = 50_000;

  private static Connection conn;
  private static Odps odps;

  @SuppressWarnings("unused") // resolved reflectively by @DisabledIf
  static boolean withoutTestIdentity() {
    return isBlank(System.getenv("odps_endpoint"))
        || isBlank(System.getenv("MAXCOMPUTE_PROJECT"))
        || isBlank(System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"))
        || isBlank(System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  @BeforeAll
  public static void setUp() throws Exception {
    conn = TestUtils.getConnection();
    odps = TestUtils.getOdps();

    Statement stmt = conn.createStatement();
    stmt.executeUpdate("drop table if exists " + TABLE);
    stmt.executeUpdate("create table " + TABLE + "(id bigint)");
    stmt.close();

    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UploadSession upload =
        tunnel.createUploadSession(odps.getDefaultProject(), TABLE);
    RecordWriter writer = upload.openRecordWriter(0);
    Record record = upload.newRecord();
    for (int i = 0; i < ROWS; i++) {
      record.setBigint(0, (long) i);
      writer.write(record);
    }
    writer.close();
    upload.commit(new Long[] {0L});
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (conn == null) {
      return;
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("drop table if exists " + TABLE);
    } finally {
      conn.close();
    }
  }

  @Test
  public void cancellingARealRunningQueryEndsItWithinADeadline() throws Exception {
    OdpsStatement stmt = (OdpsStatement) conn.createStatement();
    AtomicReference<Throwable> executeFailure = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    Thread executing = new Thread(() -> {
      started.countDown();
      try {
        stmt.executeQuery("select count(*) from " + TABLE);
      } catch (Throwable t) {
        executeFailure.set(t);
      }
    }, "live-executing");
    executing.start();
    assertTrue(started.await(60, TimeUnit.SECONDS), "the executing thread never started");

    Instance instance = awaitInstancePublished(stmt);
    assertNotNull(instance, "the driver never published an instance to cancel");

    stmt.cancel();

    executing.join(TimeUnit.SECONDS.toMillis(300));
    if (executing.isAlive()) {
      fail("execute() is still blocked 300s after cancel(), instance " + instance.getId());
    }
    awaitAtMost(300_000, instance::isTerminated,
        "instance " + instance.getId() + " to reach a terminal state after cancel()");

    Throwable t = executeFailure.get();
    if (t == null) {
      System.out.println("[live] instance " + instance.getId() + " terminated before the stop "
                         + "landed; cancel() stayed quiet, which is the point");
    } else {
      assertTrue(t instanceof SQLException,
          "a cancelled query must surface SQLException, got " + t);
      System.out.println("[live] cancelled instance " + instance.getId() + " failed execute as "
                         + t.getClass().getName() + ": " + firstLine(t.getMessage()));
    }
    stmt.close();
  }

  @Test
  public void closingAStatementDuringARealDownloadReleasesTheReadingThread() throws Exception {
    // Small splits and four readers so the download really is concurrent.
    Connection downloadConn = TestUtils.getConnection(ImmutableMap.of(
        "fetchResultSplitSize", "200",
        "fetchResultThreadNum", "4",
        "fetchResultPreloadSplitNum", "2"));
    int threadsBefore = liveDownloadPoolThreads();
    OdpsStatement stmt = (OdpsStatement) downloadConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + TABLE);

    AtomicInteger rows = new AtomicInteger();
    AtomicReference<Throwable> readFailure = new AtomicReference<>();
    CountDownLatch streaming = new CountDownLatch(1);
    Thread reader = new Thread(() -> {
      streaming.countDown();
      try {
        while (rs.next()) {
          rows.incrementAndGet();
        }
      } catch (Throwable t) {
        readFailure.set(t);
      }
    }, "live-reader");
    reader.start();
    assertTrue(streaming.await(60, TimeUnit.SECONDS), "the reader thread never started");
    awaitAtMost(120_000, () -> rows.get() >= 500,
        "the download to hand out rows (only " + rows.get() + " so far)");

    stmt.close(); // tear the statement down underneath the streaming thread

    reader.join(TimeUnit.SECONDS.toMillis(60));
    if (reader.isAlive()) {
      fail("the reading thread never came back after its statement was closed underneath it; "
           + "it had read " + rows.get() + " rows");
    }
    Throwable readFailureSeen = readFailure.get();
    System.out.println("[live] reader returned after " + rows.get() + " rows, readFailure="
                       + readFailureSeen);
    if (readFailureSeen != null) {
      assertTrue(readFailureSeen instanceof SQLException,
          "closing a statement under a live download must surface a SQLException to the reader, "
          + "got " + readFailureSeen);
    }
    downloadConn.close();
    awaitAtMost(60_000, () -> liveDownloadPoolThreads() <= threadsBefore,
        "download thread pools to be reclaimed (baseline " + threadsBefore + ", now "
        + liveDownloadPoolThreads() + ")");
  }

  @Test
  public void interleavedAndRepeatedClosesOfRealObjectsStayQuietAndRejectReuse() throws Exception {
    Connection own = TestUtils.getConnection();
    Statement stmt = own.createStatement();
    ResultSet rs = stmt.executeQuery("select * from " + TABLE + " limit 10");

    rs.close();
    rs.close();
    stmt.close();
    stmt.close();

    // JDBC: close() is idempotent, but reuse of a closed statement must be refused with
    // SQLException (it used to be a NullPointerException from the SET/USE shortcut path).
    SQLException onClosedStatement = expectSQLException(() -> stmt.executeQuery("select 1"));
    assertTrue(onClosedStatement.getMessage().toLowerCase().contains("closed"),
        String.valueOf(onClosedStatement.getMessage()));

    own.close();
    own.close();
    assertTrue(own.isClosed());
  }

  // ------------------------------------------------------------------------ helpers

  private static Instance awaitInstancePublished(OdpsStatement stmt) throws InterruptedException {
    awaitAtMost(120_000, () -> stmt.getExecuteInstance() != null,
        "the driver to publish the instance of the running query");
    return stmt.getExecuteInstance();
  }

  private static int liveDownloadPoolThreads() {
    int alive = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.isAlive() && t.getName().startsWith("pool-")) {
        alive++;
      }
    }
    return alive;
  }

  /**
   * Wait for a state change with a deadline and fail with the reason if it never happened. The
   * sleep in the loop is only a poll interval: the assertion is the condition, not the clock.
   */
  private static void awaitAtMost(long millis, BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + millis;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertTrue(condition.getAsBoolean(), "timed out after " + millis + "ms waiting for " + what);
  }

  private static String firstLine(String message) {
    if (message == null) {
      return "<null>";
    }
    int newline = message.indexOf('\n');
    return newline < 0 ? message : message.substring(0, newline);
  }

  private static SQLException expectSQLException(ThrowingRunnable action) {
    try {
      action.run();
    } catch (SQLException e) {
      return e;
    } catch (Throwable t) {
      fail("expected SQLException but got " + t);
    }
    return fail("expected SQLException, nothing was thrown");
  }

  private interface ThrowingRunnable {
    void run() throws Throwable;
  }
}
