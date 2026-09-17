package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.concurrent.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class InstanceDataIteratorCloseTest {
  private InstanceTunnel.DownloadSession session(long count) {
    InstanceTunnel.DownloadSession session = mock(InstanceTunnel.DownloadSession.class);
    when(session.getRecordCount()).thenReturn(count);
    return session;
  }

  private ExecutorService worker(InstanceDataIterator iterator) throws Exception {
    Field field = InstanceDataIterator.class.getDeclaredField("executor");
    field.setAccessible(true);
    return (ExecutorService) field.get(iterator);
  }

  private void awaitIgnoringInterrupt(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try { latch.await(); break; }
      catch (InterruptedException e) { interrupted = true; }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  @Test(timeout = 10000)
  public void closeWakesConsumerWhileReaderOpenIsStillBlocked() throws Exception {
    InstanceTunnel.DownloadSession session = session(2);
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    CountDownLatch opening = new CountDownLatch(1), release = new CountDownLatch(1);
    when(session.openRecordReader(0, 1)).thenAnswer(invocation -> {
      opening.countDown(); awaitIgnoringInterrupt(release); return reader;
    });
    InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 1, 1);
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try {
      assertTrue(opening.await(2, TimeUnit.SECONDS));
      Future<Boolean> next = consumer.submit(iterator::hasNext);
      iterator.close();
      assertFalse(next.get(2, TimeUnit.SECONDS));
      iterator.close();
    } finally {
      release.countDown(); iterator.close(); consumer.shutdownNow();
      assertTrue(worker(iterator).awaitTermination(2, TimeUnit.SECONDS));
    }
    verify(reader, times(1)).close();
    verify(session, times(1)).openRecordReader(anyLong(), anyLong());
  }

  @Test(timeout = 10000)
  public void closeUnblocksReaderAndDiscardsLateRows() throws Exception {
    InstanceTunnel.DownloadSession session = session(1);
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    Record row = mock(Record.class);
    CountDownLatch reading = new CountDownLatch(1), released = new CountDownLatch(1);
    when(session.openRecordReader(0, 1)).thenReturn(reader);
    when(reader.read()).thenAnswer(invocation -> {
      reading.countDown(); awaitIgnoringInterrupt(released); return row;
    });
    doAnswer(invocation -> { released.countDown(); return null; }).when(reader).close();
    InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 1, 1);
    try {
      assertTrue(reading.await(2, TimeUnit.SECONDS));
      iterator.close();
      assertTrue(worker(iterator).awaitTermination(2, TimeUnit.SECONDS));
      assertFalse(iterator.hasNext());
      iterator.close();
      verify(reader, times(1)).close();
    } finally { released.countDown(); iterator.close(); }
  }

  @Test(timeout = 10000)
  public void closeDoesNotSubmitNextSplit() throws Exception {
    InstanceTunnel.DownloadSession session = session(2);
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    CountDownLatch finished = new CountDownLatch(1);
    when(session.openRecordReader(0, 1)).thenReturn(reader);
    doAnswer(invocation -> { finished.countDown(); return null; }).when(reader).close();
    InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 1, 1);
    try {
      assertTrue(finished.await(2, TimeUnit.SECONDS));
      iterator.close();
      assertFalse(iterator.hasNext());
      verify(session, times(1)).openRecordReader(anyLong(), anyLong());
    } finally { iterator.close(); }
  }

  @Test(timeout = 10000)
  public void emptyResultCanBeReadAndClosedRepeatedly() {
    InstanceDataIterator iterator = new InstanceDataIterator(session(0), 0, null, 0, -1, -1);
    assertFalse(iterator.hasNext());
    iterator.close(); iterator.close();
    assertFalse(iterator.hasNext());
  }

  @Test(timeout = 10000)
  public void queuedSplitsAreCancelledAndConcurrentCloseReleasesOnce() throws Exception {
    InstanceTunnel.DownloadSession session = session(3);
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    CountDownLatch reading = new CountDownLatch(1), released = new CountDownLatch(1);
    when(session.openRecordReader(0, 1)).thenReturn(reader);
    when(reader.read()).thenAnswer(invocation -> {
      reading.countDown(); awaitIgnoringInterrupt(released); return null;
    });
    doAnswer(invocation -> { released.countDown(); return null; }).when(reader).close();
    InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 3, 1);
    ExecutorService closers = Executors.newFixedThreadPool(2);
    try {
      assertTrue(reading.await(2, TimeUnit.SECONDS));
      Future<?> a = closers.submit(iterator::close), b = closers.submit(iterator::close);
      a.get(2, TimeUnit.SECONDS); b.get(2, TimeUnit.SECONDS);
      assertTrue(worker(iterator).awaitTermination(2, TimeUnit.SECONDS));
      assertFalse(iterator.hasNext());
      verify(session, times(1)).openRecordReader(anyLong(), anyLong());
      verify(reader, times(1)).close();
    } finally { released.countDown(); iterator.close(); closers.shutdownNow(); }
  }

  @Test(timeout = 10000)
  public void downloadFailureWakesConsumerAndRetainsCause() throws Exception {
    InstanceTunnel.DownloadSession session = session(2);
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    java.io.IOException failure = new java.io.IOException("read failed");
    when(session.openRecordReader(0, 1)).thenReturn(reader);
    when(reader.read()).thenThrow(failure);
    InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 1, 1);
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try {
      try {
        consumer.submit(iterator::hasNext).get(2, TimeUnit.SECONDS);
        fail("Expected download failure");
      } catch (ExecutionException e) { assertSame(failure, e.getCause().getCause()); }
      assertTrue(worker(iterator).awaitTermination(2, TimeUnit.SECONDS));
      verify(reader, times(1)).close();
    } finally { iterator.close(); consumer.shutdownNow(); }
  }

  @Test(timeout = 10000)
  public void normalSplitTraversalPreservesRows() throws Exception {
    InstanceTunnel.DownloadSession session = session(2);
    TunnelRecordReader first = mock(TunnelRecordReader.class), second = mock(TunnelRecordReader.class);
    Record a = mock(Record.class), b = mock(Record.class);
    when(session.openRecordReader(0, 1)).thenReturn(first);
    when(session.openRecordReader(1, 1)).thenReturn(second);
    when(first.read()).thenReturn(a, null); when(second.read()).thenReturn(b, null);
    try (InstanceDataIterator iterator = new InstanceDataIterator(session, 0, null, 1, 1, 1)) {
      assertTrue(iterator.hasNext()); assertSame(a, iterator.next());
      assertTrue(iterator.hasNext()); assertSame(b, iterator.next());
      assertFalse(iterator.hasNext());
    }
    verify(first, timeout(2000).times(1)).close();
    verify(second, timeout(2000).times(1)).close();
  }
}
