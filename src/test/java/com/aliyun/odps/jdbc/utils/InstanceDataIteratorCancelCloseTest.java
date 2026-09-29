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

package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.type.TypeInfoFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cancel / timeout / close races of the concurrent instance-result downloader.
 *
 * <p>Everything here is driven by latches and by a split source that stalls or fails on
 * command, so a pass does not depend on how fast this machine happens to be. The one thing
 * deliberately NOT asserted anywhere below is how many records of a split reached the client
 * before a failure: a sharded download can hand out a prefix and then lose the rest, and the
 * driver makes no exactly-once promise about it.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
public class InstanceDataIteratorCancelCloseTest {

  private static final TableSchema SCHEMA = buildSchema();
  /** Upper bound for "the thread is gone"; before the fix under test these threads never come back. */
  private static final long PARKED_JOIN_MILLIS = 30_000;

  private static TableSchema buildSchema() {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", TypeInfoFactory.BIGINT));
    return schema;
  }

  private static Record row(long id) {
    ArrayRecord record = new ArrayRecord(SCHEMA);
    record.set(0, id);
    return record;
  }

  /**
   * A split source whose readers do exactly what a test tells them to: serve records, stall
   * (a slow tunnel response), or fail (a broken download).
   */
  private static final class ControllableSource implements InstanceDataIterator.DownloadSource {
    private final long recordCount;
    private final AtomicInteger openCalls = new AtomicInteger();
    private final AtomicInteger closeCalls = new AtomicInteger();
    /** Signalled every time a reader is (re)entered, so a test can wait for "inside read". */
    private final CountDownLatch enteredRead = new CountDownLatch(1);
    /** Released by the test to let stalled readers finish. */
    private final CountDownLatch releaseReaders = new CountDownLatch(1);
    private volatile Answer<Record> behaviour;

    ControllableSource(long recordCount) {
      this.recordCount = recordCount;
    }

    @Override
    public long getRecordCount() {
      return recordCount;
    }

    @Override
    public TableSchema getSchema() {
      return SCHEMA;
    }

    @Override
    public TunnelRecordReader open(long start, long count) throws Exception {
      openCalls.incrementAndGet();
      TunnelRecordReader reader = Mockito.mock(TunnelRecordReader.class);
      Mockito.when(reader.read()).thenAnswer(behaviour);
      Mockito.doAnswer(invocation -> {
        closeCalls.incrementAndGet();
        return null;
      }).when(reader).close();
      return reader;
    }

    /** Serve every record, then stall until {@link #releaseReaders()} is called. */
    void thenStall() {
      behaviour = invocation -> {
        enteredRead.countDown();
        releaseReaders.await();
        return null;
      };
    }

    /** Serve one record, then fail the way a broken tunnel response does. */
    void thenFailAfterFirstRecord(final IOException failure) {
      AtomicInteger served = new AtomicInteger();
      behaviour = invocation -> {
        enteredRead.countDown();
        if (served.getAndIncrement() == 0) {
          return row(1);
        }
        throw failure;
      };
    }

    void releaseReaders() {
      releaseReaders.countDown();
    }

    boolean awaitEnteredRead() throws InterruptedException {
      return enteredRead.await(30, TimeUnit.SECONDS);
    }

    void awaitOpenCalls(final int expected) {
      awaitUntil(() -> openCalls.get() >= expected, expected + " split readers should be open");
    }
  }

  private static void awaitUntil(BooleanSupplier condition, String what) {
    long deadline = System.currentTimeMillis() + 30_000;
    while (!condition.getAsBoolean()) {
      assertTrue(System.currentTimeMillis() < deadline, "timed out waiting: " + what);
      Thread.yield();
    }
  }

  private static void awaitParked(Thread victim) throws InterruptedException {
    // Wait for the observable precondition (the thread is no longer running driver code) rather
    // than for an arbitrary wall-clock delay.
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (!victim.isAlive()
          || victim.getState() == Thread.State.WAITING
          || victim.getState() == Thread.State.TIMED_WAITING) {
        return;
      }
      Thread.yield();
    }
  }

  @Test
  public void closeReleasesAConsumerParkedBehindASplitThatNeverCompletes() throws Exception {
    ControllableSource source = new ControllableSource(100);
    source.thenStall();
    // one split, one worker: nothing can be handed to the consumer while that worker is stalled
    final InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 100, 1, 1);

    final CountDownLatch startReading = new CountDownLatch(1);
    final Throwable[] thrown = new Throwable[1];
    Thread consumer = new Thread(() -> {
      startReading.countDown();
      try {
        iterator.hasNext();
      } catch (Throwable t) {
        thrown[0] = t;
      }
    }, "parked-consumer");
    consumer.start();
    assertTrue(startReading.await(30, TimeUnit.SECONDS), "consumer did not start");
    assertTrue(source.awaitEnteredRead(), "no reader was entered");
    awaitParked(consumer);

    iterator.close();

    consumer.join(PARKED_JOIN_MILLIS);
    assertTrue(!consumer.isAlive(),
        "close() did not release the thread parked in hasNext(); it never comes back");
    assertInstanceOf(IllegalStateException.class, thrown[0],
        "a released consumer must be told the iterator is closed, not handed a fake download "
        + "failure");
    source.releaseReaders();
    assertTrue(iterator.downloadThreadsTerminated(), "download threads must terminate");
  }

  @Test
  public void closingASplitThatIsStillStalledDoesNotInventADownloadFailure() throws Exception {
    ControllableSource source = new ControllableSource(100);
    source.thenStall();
    final InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 100, 1, 1);
    assertTrue(source.awaitEnteredRead(), "no reader was entered");

    Thread closer = new Thread(iterator::close, "closer");
    closer.start();
    // Nobody releases the stalled reader: close() has to interrupt it, and the interrupt it
    // caused must not come back as a user-visible "the tunnel broke" report.
    closer.join(PARKED_JOIN_MILLIS);
    assertTrue(!closer.isAlive(), "close() did not return");
    awaitUntil(iterator::downloadThreadsTerminated, "download threads to terminate");
    assertEquals(1, source.closeCalls.get(), "the stalled reader must be closed exactly once");

    // A fresh read after close reports the closed state, never a "Download failed" cause.
    assertThrows(IllegalStateException.class, iterator::hasNext);
    assertNull(iterator.downloadFailure(),
        "the interrupt induced by close() must not be remembered as a download failure; the next "
        + "reader would be told the tunnel broke when it did not");
    source.releaseReaders();
  }

  @Test
  public void closeDoesNotWaitBehindASplitItHasAlreadyDiscarded() throws Exception {
    ControllableSource source = new ControllableSource(100);
    source.thenStall(); // the reader only reacts to the interrupt, never to a polite shutdown
    InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 100, 1, 1);
    assertTrue(source.awaitEnteredRead(), "no reader was entered");

    long began = System.nanoTime();
    iterator.close();
    long millis = (System.nanoTime() - began) / 1_000_000L;

    assertTrue(millis < 2_000L,
        "close() waited " + millis + "ms for a split whose data nobody wants any more; waiting "
        + "politely for it costs the caller the whole grace period");
    assertTrue(iterator.downloadThreadsTerminated(), "download threads must terminate");
    source.releaseReaders();
  }

  @Test
  public void downloadFailureKeepsItsCauseAndReleasesEveryResource() throws Exception {
    IOException injected = new IOException("tunnel reset in the middle of a split");
    ControllableSource source = new ControllableSource(100);
    source.thenFailAfterFirstRecord(injected);
    InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 100, 1, 1);

    assertTrue(iterator.hasNext(), "the record served before the failure must still arrive");
    iterator.next();
    RuntimeException failure = assertThrows(RuntimeException.class, iterator::hasNext);
    assertEquals("Download failed", failure.getMessage());
    assertSame(injected, failure.getCause(),
        "the caller must be able to reach the injected failure through getCause()");

    iterator.close();
    assertTrue(iterator.downloadThreadsTerminated(), "download threads must terminate");
    assertEquals(1, source.closeCalls.get(), "the failed reader must be closed");
  }

  @Test
  public void closeBeforeAnythingIsReadReleasesPreallocatedSplits() throws Exception {
    ControllableSource source = new ControllableSource(100);
    source.thenStall();
    // preload 2 splits: two readers are in flight although nobody read a single record
    InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 50, 2, 2);
    source.awaitOpenCalls(2);

    source.releaseReaders();
    iterator.close();
    assertTrue(iterator.downloadThreadsTerminated(), "download threads must terminate");
    assertEquals(2, source.closeCalls.get(), "every preloaded reader must be closed");

    iterator.close();
    assertTrue(iterator.downloadThreadsTerminated(), "repeated close must stay a no-op");
  }

  @Test
  public void drainingAllRecordsAndThenClosingIsStillANormalPath() throws Exception {
    ControllableSource source = new ControllableSource(3);
    AtomicInteger served = new AtomicInteger();
    source.behaviour = invocation -> {
      int n = served.getAndIncrement();
      return n < 3 ? row(n) : null;
    };
    InstanceDataIterator iterator = new InstanceDataIterator(source, 0, null, 3, 1, 1);

    int seen = 0;
    while (iterator.hasNext()) {
      iterator.next();
      seen++;
    }
    assertEquals(3, seen, "the whole result must stream when nothing cancels it");
    iterator.close();
    assertTrue(iterator.downloadThreadsTerminated(), "download threads must terminate");
  }
}
