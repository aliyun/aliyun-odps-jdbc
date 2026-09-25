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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.type.TypeInfoFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cancel / timeout / close contract of a statement, with the server replaced by mocks so every
 * interleaving is forced instead of raced for. No thread sleeps anywhere: each test parks the
 * other thread on a latch, acts, and then releases it.
 *
 * <p>JDBC lets a driver ignore cancel() when nothing is executing. What it does not allow is a
 * cancel() silently dropped while a query IS executing, or a close() turning a concurrent
 * cancel() into a NullPointerException. Those are the behaviours pinned below.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
public class OdpsStatementCancelCloseTest {

  private static final OdpsLogger LOG =
      new OdpsLogger("OdpsStatementCancelCloseTest", null, null, null, false, false, null);

  private OdpsConnection conn;
  private SQLExecutor executor;
  private OdpsStatement stmt;

  private void givenStatement(ExecuteMode mode) throws Exception {
    conn = Mockito.mock(OdpsConnection.class);
    conn.log = LOG;
    executor = Mockito.mock(SQLExecutor.class);
    SQLExecutorBuilder builder = Mockito.mock(SQLExecutorBuilder.class);
    Mockito.when(builder.clone()).thenReturn(builder);
    Mockito.when(conn.getExecutor()).thenReturn(executor);
    Mockito.when(conn.getExecutorBuilder()).thenReturn(builder);
    Mockito.when(conn.getSqlTaskProperties()).thenReturn(new Properties());
    Mockito.when(conn.getInteractiveMode()).thenReturn(mode);
    Mockito.when(conn.runningInInteractiveMode()).thenReturn(mode != ExecuteMode.OFFLINE);
    Mockito.when(conn.enableLimit()).thenReturn(true);
    Mockito.when(conn.getCountLimit()).thenReturn(100L);
    Mockito.when(conn.getSizeLimit()).thenReturn(100L);
    Mockito.when(executor.getExecuteMode()).thenReturn(ExecuteMode.OFFLINE);
    Mockito.when(executor.isUseInstanceTunnel()).thenReturn(true);
    Mockito.when(executor.getExecutionLog()).thenReturn(Collections.<String>emptyList());
    Mockito.when(conn.createSQLException(Mockito.anyString(), Mockito.any(Exception.class)))
        .thenAnswer(inv -> new SQLException((String) inv.getArgument(0),
                                            (Exception) inv.getArgument(1)));
    stmt = new OdpsStatement(conn);
  }

  private static OdpsResultSetMetaData singleColumnMeta() {
    return new OdpsResultSetMetaData(Collections.singletonList("id"),
                                     Collections.singletonList(TypeInfoFactory.BIGINT));
  }

  // -------------------------------------------------------------- cancel 前 / cancel 后

  @Test
  public void cancelBeforeAnythingIsExecutedIsANoOp() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);

    stmt.cancel();

    assertFalse(stmt.isCancelled, "an idle cancel() must not mark the statement cancelled");
    Mockito.verifyNoInteractions(executor);

    stmt.cancel();
    assertFalse(stmt.isCancelled, "repeated idle cancel() must stay a no-op");
  }

  @Test
  public void cancelAfterTheQueryAlreadyFinishedDoesNotAskTheServerToStopIt() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_already_terminated");
    Mockito.when(instance.isTerminated()).thenReturn(true);
    stmt.executeInstance = instance;

    stmt.cancel();

    Mockito.verify(instance, Mockito.never()).stop();
    assertTrue(stmt.isCancelled, "cancel() after completion still reports the cancel");
  }

  @Test
  public void repeatedCancelStopsTheRunningInstanceOnlyOnce() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_running");
    Mockito.when(instance.isTerminated()).thenReturn(false);
    stmt.executeInstance = instance;

    stmt.cancel();
    stmt.cancel();
    stmt.cancel();

    Mockito.verify(instance, Mockito.times(1)).stop();
  }

  @Test
  public void stopThatLosesTheRaceWithTerminationIsNotReportedAsACancelFailure() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    // The instance terminates between the status check and stop(). The caller asked for it to
    // stop, so this is a success, not an error to report.
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_racing_termination");
    Mockito.when(instance.isTerminated()).thenReturn(false, true);
    Mockito.doThrow(new OdpsException("ODPS-0412111: Invalid state setting")).when(instance).stop();
    stmt.executeInstance = instance;

    stmt.cancel();

    Mockito.verify(instance).stop();
    assertTrue(stmt.isCancelled);
  }

  @Test
  public void aRealStopFailureStillSurfacesAsSQLException() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_running");
    Mockito.when(instance.isTerminated()).thenReturn(false);
    Mockito.doThrow(new OdpsException("ODPS-0130161: no permission to stop this job"))
        .when(instance).stop();
    stmt.executeInstance = instance;

    SQLException thrown = assertThrows(SQLException.class, stmt::cancel);

    assertTrue(thrown.getMessage().contains("no permission"), thrown.getMessage());
    assertFalse(stmt.isCancelled, "a cancel the server refused must not be remembered as done");
  }

  // ------------------------------------------------- cancel racing execute and close

  @Test
  public void cancelIssuedWhileTheQueryIsStillBeingSubmittedIsNotDropped() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    final CountDownLatch insideRun = new CountDownLatch(1);
    final CountDownLatch releaseRun = new CountDownLatch(1);
    final Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_submitted_during_cancel");
    Mockito.when(instance.isTerminated()).thenReturn(false);
    Mockito.doAnswer(inv -> {
      insideRun.countDown();
      releaseRun.await();
      return null;
    }).when(executor).run(Mockito.anyString(), Mockito.anyMap());
    Mockito.when(executor.getInstance()).thenReturn(instance);

    final SQLException[] executeFailure = new SQLException[1];
    Thread executing = new Thread(() -> {
      try {
        stmt.executeQuery("select 1");
      } catch (SQLException e) {
        executeFailure[0] = e;
      }
    }, "executing-thread");
    executing.start();
    assertTrue(insideRun.await(30, TimeUnit.SECONDS), "execute() never reached the SDK");

    stmt.cancel();
    assertTrue(stmt.isCancelled, "the cancel issued mid-submission must be remembered");
    assertNull(stmt.executeInstance, "the instance only exists once the submission returned");

    releaseRun.countDown();
    executing.join(30_000);
    assertFalse(executing.isAlive(), "the executing thread must come back");

    Mockito.verify(instance).stop();
    assertInstanceOf(SQLException.class, executeFailure[0],
        "a query cancelled during submission must fail its execute() instead of returning the "
        + "result of a job nobody waits for any more");
    assertTrue(executeFailure[0].getMessage().contains("cancel"),
        String.valueOf(executeFailure[0].getMessage()));
  }

  @Test
  public void cancelAfterCloseThrowsSQLExceptionNotNullPointerException() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_running");
    Mockito.when(instance.isTerminated()).thenReturn(false);
    stmt.executeInstance = instance;
    stmt.close();

    assertThrows(SQLException.class, stmt::cancel);
    Mockito.verify(instance, Mockito.never()).stop();
  }

  @Test
  public void closeWhileACancelIsInFlightCannotBreakIntoANullPointerException() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    final CountDownLatch insideStop = new CountDownLatch(1);
    final CountDownLatch releaseStop = new CountDownLatch(1);
    Instance instance = Mockito.mock(Instance.class);
    Mockito.when(instance.getId()).thenReturn("instance_running");
    Mockito.when(instance.isTerminated()).thenReturn(false);
    // stop() only completes after close() has already torn the statement down underneath it.
    Mockito.doAnswer(inv -> {
      insideStop.countDown();
      releaseStop.await(30, TimeUnit.SECONDS);
      return null;
    }).when(instance).stop();
    stmt.executeInstance = instance;

    final Throwable[] cancelFailure = new Throwable[1];
    Thread cancelling = new Thread(() -> {
      try {
        stmt.cancel();
      } catch (Throwable t) {
        cancelFailure[0] = t;
      }
    }, "cancelling-thread");
    cancelling.start();
    assertTrue(insideStop.await(30, TimeUnit.SECONDS), "cancel() never reached stop()");

    stmt.close();
    releaseStop.countDown();
    cancelling.join(30_000);
    assertFalse(cancelling.isAlive(), "the cancelling thread must come back");

    Throwable t = cancelFailure[0];
    if (t != null) {
      assertInstanceOf(SQLException.class, t,
          "a cancel() overlapping close() must report a SQLException, got " + t);
    }
    Mockito.verify(instance).stop();
  }

  @Test
  public void cancellingAMaxQAQueryStillRunningInsideTheSdkReachesTheExecutor() throws Exception {
    givenStatement(ExecuteMode.INTERACTIVE_V2);
    final CountDownLatch insideRun = new CountDownLatch(1);
    final CountDownLatch releaseRun = new CountDownLatch(1);
    // A MaxQA subquery that has not produced an offline instance yet: the old code had nothing
    // to cancel and returned quietly while the query kept running.
    Mockito.when(executor.getInstance()).thenReturn(null);
    Mockito.doAnswer(inv -> {
      insideRun.countDown();
      releaseRun.await();
      return null;
    }).when(executor).run(Mockito.anyString(), Mockito.anyMap());

    Thread executing = new Thread(() -> {
      try {
        stmt.execute("select 1");
      } catch (SQLException ignored) {
        // the pending cancel makes execute() fail; asserted below via the executor
      }
    }, "maxqa-executing");
    executing.start();
    assertTrue(insideRun.await(30, TimeUnit.SECONDS), "execute() never reached the SDK");

    stmt.cancel();
    Mockito.verify(executor).cancel();

    releaseRun.countDown();
    executing.join(30_000);
    assertFalse(executing.isAlive(), "the executing thread must come back");
  }

  // -------------------------------------------------- interleaved / repeated close

  @Test
  public void resultSetAndStatementCanBeClosedInAnyOrderAndRepeatedly() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    stmt.inputProperties = new Properties();
    final AtomicInteger released = new AtomicInteger();
    com.aliyun.odps.data.ResultSet odpsResultSet = countingResultSet(released);
    OdpsSessionForwardResultSet resultSet =
        new OdpsSessionForwardResultSet(stmt, singleColumnMeta(), odpsResultSet,
                                        System.currentTimeMillis());
    stmt.resultSet = resultSet;

    resultSet.close();
    resultSet.close();
    stmt.close();
    stmt.close();
    resultSet.close();

    assertEquals(1, released.get(), "the download must be released exactly once");
    assertTrue(stmt.isClosed());
    assertTrue(resultSet.isClosed());
    Mockito.verify(conn, Mockito.never()).close();
  }

  @Test
  public void closingTheStatementReleasesAResultSetTheCallerNeverSaw() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    final AtomicInteger released = new AtomicInteger();
    stmt.odpsResultSet = countingResultSet(released);

    stmt.close();
    stmt.close();

    assertEquals(1, released.get(),
        "a result set that was never wrapped in a java.sql.ResultSet must still be released once");
  }

  @Test
  public void aClosedStatementRefusesToRunAndItsCancelIsRejected() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    stmt.close();

    assertThrows(SQLException.class, () -> stmt.execute("select 1"));
    assertThrows(SQLException.class, () -> stmt.executeQuery("select 1"));
    assertThrows(SQLException.class, () -> stmt.executeUpdate("select 1"));
    assertThrows(SQLException.class, () -> stmt.cancel());
    assertTrue(stmt.isClosed());
  }

  @Test
  public void closingTheStatementReleasesItsOwnExecutorButNotTheSharedConnectionOne()
      throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    SQLExecutor statementOwned = Mockito.mock(SQLExecutor.class);
    SQLExecutorBuilder builder = Mockito.mock(SQLExecutorBuilder.class);
    Mockito.when(builder.clone()).thenReturn(builder);
    Mockito.when(builder.build()).thenReturn(statementOwned);
    Mockito.when(conn.getExecutorBuilder()).thenReturn(builder);

    // "set jdbc.fetchResult.useTunnel=false" rebuilds a statement-owned SQLExecutor.
    stmt.execute("set jdbc.fetchResult.useTunnel=false;");
    Mockito.verify(conn, Mockito.times(1)).getExecutorBuilder();
    stmt.close();

    Mockito.verify(statementOwned).close();
    Mockito.verify(executor, Mockito.never()).close();
  }

  @Test
  public void aResultReadRacingWithCloseSurfacesAsSQLException() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    stmt.inputProperties = new Properties();
    // The download behind this result set is closed by someone else mid-read, which is what
    // InstanceDataIterator reports when a statement / connection close lands during a read.
    com.aliyun.odps.data.ResultSet closedUnderneath = throwingResultSet(
        new IllegalStateException("InstanceDataIterator is already closed"));
    ResultSet rs = new OdpsSessionForwardResultSet(stmt, singleColumnMeta(), closedUnderneath,
                                                   System.currentTimeMillis());

    SQLException thrown = assertThrows(SQLException.class, rs::next);

    assertTrue(thrown.getMessage().toLowerCase().contains("closed"), thrown.getMessage());
    assertInstanceOf(IllegalStateException.class, thrown.getCause(),
        "the real reason must stay reachable through getCause()");
  }

  @Test
  public void aFailedSplitReachesTheCallerAsSQLExceptionKeepingTheOriginalCause() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);
    stmt.inputProperties = new Properties();
    IOException tunnelBroke = new IOException("tunnel reset mid-split");
    ResultSet rs = new OdpsSessionForwardResultSet(
        stmt, singleColumnMeta(),
        throwingResultSet(new RuntimeException("Download failed", tunnelBroke)),
        System.currentTimeMillis());

    SQLException thrown = assertThrows(SQLException.class, rs::next);

    assertInstanceOf(RuntimeException.class, thrown.getCause());
    assertSame(tunnelBroke, thrown.getCause().getCause(),
        "the injected download failure must be the root cause, not a wrapper invented on the way");
  }

  private static com.aliyun.odps.data.ResultSet throwingResultSet(final RuntimeException failure) {
    return new com.aliyun.odps.data.ResultSet(
        Collections.<com.aliyun.odps.data.Record>emptyList().iterator(), new TableSchema(), 0L) {
      @Override
      public boolean hasNext() {
        throw failure;
      }
    };
  }

  // --------------------------------------------------------------------- query timeout

  @Test
  public void queryTimeoutIsPushedToTheMaxQASettings() throws Exception {
    givenStatement(ExecuteMode.INTERACTIVE_V2);
    stmt.setQueryTimeout(7);

    stmt.execute("select 1");

    Map<String, String> settings = capturedSettings();
    assertEquals("7", settings.get("odps.sql.maxqa.query.timeout"));
    assertEquals(7, stmt.getQueryTimeout());
  }

  @Test
  public void anExplicitHintWinsOverStatementQueryTimeout() throws Exception {
    givenStatement(ExecuteMode.INTERACTIVE_V2);
    stmt.setQueryTimeout(7);

    stmt.execute("set odps.sql.maxqa.query.timeout=99; select 1");

    assertEquals("99", capturedSettings().get("odps.sql.maxqa.query.timeout"),
        "Statement.setQueryTimeout must not overwrite an explicit per-query hint");
  }

  @Test
  public void offlineModeDoesNotPretendToEnforceAQueryTimeout() throws Exception {
    givenStatement(ExecuteMode.OFFLINE);

    stmt.setQueryTimeout(3);

    assertEquals(-1, stmt.queryTimeout,
        "offline setQueryTimeout() is documented as unsupported: it must not record a value "
        + "the driver then never enforces");
    // getQueryTimeout() refuses to report a value the offline path cannot honour either.
    assertThrows(java.sql.SQLFeatureNotSupportedException.class, () -> stmt.getQueryTimeout());
  }

  @Test
  public void invalidQueryTimeoutValuesAreRejected() throws Exception {
    givenStatement(ExecuteMode.INTERACTIVE);

    assertThrows(IllegalArgumentException.class, () -> stmt.setQueryTimeout(0));
    assertThrows(IllegalArgumentException.class, () -> stmt.setQueryTimeout(-5));
    assertEquals(-1, stmt.getQueryTimeout(), "unset means no timeout");
  }

  private Map<String, String> capturedSettings() throws Exception {
    ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(executor).run(Mockito.anyString(), captor.capture());
    return (Map<String, String>) captor.getValue();
  }

  private static com.aliyun.odps.data.ResultSet countingResultSet(final AtomicInteger released) {
    return new com.aliyun.odps.data.ResultSet(
        Collections.<com.aliyun.odps.data.Record>emptyList().iterator(), new TableSchema(), 0L) {
      @Override
      public void close() {
        released.incrementAndGet();
      }
    };
  }
}
