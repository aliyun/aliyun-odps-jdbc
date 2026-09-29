package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.sqa.SQLExecutorConstants;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;

/**
 * Use sharded concurrent download mode to download sample data.
 * Variable parameter split size, and number of preload splits.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InstanceDataIterator implements Iterator<Record>, AutoCloseable {

  private static final Record EOF_RECORD = new EOFRecord();
  /** Upper bound on how long {@link #close()} waits for a stalled split reader. */
  static final int CLOSE_AWAIT_TERMINATION_SECONDS = 5;
  private static final OdpsLogger LOG = new OdpsLogger(InstanceDataIterator.class.getName(), null, null, null, false, false, null);
  private boolean isSelect = true;

  private ExecutorService executor;
  private int splitNum;
  private BlockingQueue<Record>[] queues;
  private AtomicReference<Throwable> error = new AtomicReference<>();
  private AtomicBoolean closed = new AtomicBoolean(false);
  private long offset;
  private long recordCount;
  private int preloadSplitNum;
  private long splitSize;
  private DownloadSource source;
  private int threadNum;

  private int currentSplit = 0;

  private Record currentRecord;

  public InstanceDataIterator(Odps odps, Instance instance, long offset, Long readCount,
      long splitSize, int preloadSplitNum, int threadNum) throws OdpsException {
    this(odps, instance, offset, readCount, splitSize, preloadSplitNum, threadNum, null);
  }

  public InstanceDataIterator(Odps odps, Instance instance, long offset, Long readCount,
      long splitSize, int preloadSplitNum, int threadNum, String tunnelQuotaName)
      throws OdpsException {
    final InstanceTunnel.DownloadSession session;
    try {
      session = createInstanceTunnel(odps, tunnelQuotaName)
          .createDownloadSession(instance.getProject(), instance.getId(), false);
    } catch (TunnelException e) {
      if (e.getErrorCode().equals(SQLExecutorConstants.sessionNotSelectException)
          || e.getErrorMsg().contains(SQLExecutorConstants.sessionNotSelectMessage)) {
        isSelect = false;
        currentRecord = getInfoRecord(instance);
        return;
      }
      throw e;
    }
    init(new SessionSource(session), offset, readCount, splitSize, preloadSplitNum, threadNum);
  }

  /**
   * Seam used by the cancel / close regressions: drive the very same producer - consumer
   * state machine with a split source that can stall or fail on demand, instead of
   * gambling on a live tunnel timing out.
   */
  InstanceDataIterator(DownloadSource source, long offset, Long readCount, long splitSize,
                       int preloadSplitNum, int threadNum) {
    init(source, offset, readCount, splitSize, preloadSplitNum, threadNum);
  }

  /** The two calls this iterator makes on a download session, plus its row count. */
  interface DownloadSource {
    long getRecordCount();

    TableSchema getSchema();

    TunnelRecordReader open(long start, long count) throws Exception;
  }

  private static final class SessionSource implements DownloadSource {
    private final InstanceTunnel.DownloadSession session;

    SessionSource(InstanceTunnel.DownloadSession session) {
      this.session = session;
    }

    @Override
    public long getRecordCount() {
      return session.getRecordCount();
    }

    @Override
    public TableSchema getSchema() {
      return session.getSchema();
    }

    @Override
    public TunnelRecordReader open(long start, long count) throws Exception {
      return session.openRecordReader(start, count);
    }
  }

  private void init(DownloadSource source, long offset, Long readCount, long splitSize,
                    int preloadSplitNum, int threadNum) {
    this.source = source;
    this.offset = offset;
    this.recordCount = (readCount == null || readCount < 0) ? source.getRecordCount() - offset : Math.min(readCount, (source.getRecordCount() - offset));
    this.splitSize = (splitSize <= 0) ? this.recordCount : splitSize;
    this.splitNum = computeSplitNum(this.splitSize, recordCount);
    this.preloadSplitNum = (preloadSplitNum == -1) ? splitNum : Math.max(preloadSplitNum, 1);
    this.threadNum = (threadNum == -1) ? Math.min(this.preloadSplitNum, Runtime.getRuntime()
                                                                            .availableProcessors() * 2) : threadNum;

    this.executor = Executors.newFixedThreadPool(this.threadNum);
    this.queues = new LinkedBlockingQueue[this.splitNum];

    // Initialize first batch of splits
    for (int i = 0; i < this.preloadSplitNum && i < this.splitNum; i++) {
      submitNextSplit(i);
    }
  }


  static InstanceTunnel createInstanceTunnel(Odps odps, String tunnelQuotaName) {
    InstanceTunnel tunnel = new InstanceTunnel(odps);
    if (!StringUtils.isNullOrEmpty(tunnelQuotaName)) {
      ((com.aliyun.odps.tunnel.Configuration) tunnel.getConfig()).setQuotaName(tunnelQuotaName);
    }
    return tunnel;
  }

  private int computeSplitNum(long splitSize, long recordCount) {
    return (int) ((recordCount + splitSize - 1) / splitSize);
  }

  private void submitNextSplit(final int splitIndex) {
    final BlockingQueue<Record>[] liveQueues = queues;
    if (splitIndex >= splitNum || closed.get() || liveQueues == null) {
      return;
    }

    final long start = offset + (long) splitIndex * splitSize;
    final long count = Math.min(splitSize, recordCount - ((long) splitIndex * splitSize));
    // The queue is captured by the task instead of being re-read from the array: close()
    // drops the array slots, and a worker that then dereferenced them used to fail the
    // split with an NPE that was reported as a download failure.
    final BlockingQueue<Record> queue = new LinkedBlockingQueue<>();
    liveQueues[splitIndex] = queue;
    try {
      executor.submit(() -> downloadSplit(splitIndex, start, count, queue));
    } catch (RejectedExecutionException rejected) {
      // close() shut the pool down between the check above and this submit. Release the
      // consumer that would otherwise park on this queue and forget the slot.
      liveQueues[splitIndex] = null;
      queue.offer(EOF_RECORD);
      if (!closed.get()) {
        error.compareAndSet(null, rejected);
      }
    }
  }

  private void downloadSplit(int splitIndex, long start, long count,
                             BlockingQueue<Record> queue) {
    TunnelRecordReader reader = null;
    try {
      reader = source.open(start, count);
      Record record;
      while ((record = reader.read()) != null) {
        if (closed.get()) {
          return; // the consumer is gone; the finally block still releases the reader
        }
        queue.put(record);
      }
      if (!closed.get()) {
        queue.put(EOF_RECORD);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (!closed.get()) {
        error.compareAndSet(null, e);
        LOG.error("Interrupted while downloading split index " + splitIndex, e);
        queue.offer(EOF_RECORD);
      }
    } catch (Throwable t) {
      if (!closed.get()) {
        error.compareAndSet(null, t);
        LOG.error("Error reading from split index " + splitIndex, t);
      }
      // Never leave a consumer parked behind a split that will not produce more data.
      queue.offer(EOF_RECORD);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.warn("Failed to close TunnelRecordReader for split " + splitIndex + ": " + e.getMessage());
        }
      }
    }
  }

  private boolean hasNextInternal() {
    checkClosed();
    checkError();
    if (currentSplit >= splitNum) {
      currentRecord = EOF_RECORD;
      return false;
    }
    BlockingQueue<Record> currentQueue = queues[currentSplit];
    if (currentQueue == null) {
      // Queue already consumed, move to next split
      currentSplit++;
      return hasNextInternal();
    }
    try {
      Record record = currentQueue.take();
      if (closed.get()) {
        // close() released this park with a sentinel rather than with split data.
        throw new IllegalStateException("InstanceDataIterator is already closed");
      }
      if (record == EOF_RECORD) {
        queues[currentSplit] = null; // Help GC collect the queue
        submitNextSplit(currentSplit + preloadSplitNum); // Submit next split after current is done
        currentSplit++;
        return hasNextInternal();
      }
      this.currentRecord = record;
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during read", e);
    }
  }

  @Override
  public boolean hasNext() {
    if (isSelect) {
      return hasNextInternal();
    } else {
      return currentRecord != EOF_RECORD;
    }
  }

  @Override
  public Record next() {
    if (currentRecord == EOF_RECORD) {
      throw new NoSuchElementException("No more records.");
    }
    if (isSelect) {
      return currentRecord;
    } else {
      Record record = currentRecord;
      currentRecord = EOF_RECORD;
      return record;
    }
  }

  private void checkError() {
    if (error.get() != null) {
      throw new RuntimeException("Download failed", error.get());
    }
  }

  private void checkClosed() {
    if (closed.get()) {
      throw new IllegalStateException("InstanceDataIterator is already closed");
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // Release a consumer parked in take() *before* the queues are dropped. A split whose
    // reader was stalled never delivers its EOF sentinel once `closed` is set, so dropping
    // the queues first left the reading thread parked there forever.
    final BlockingQueue<Record>[] liveQueues = queues;
    if (liveQueues != null) {
      for (int i = 0; i < liveQueues.length; i++) {
        BlockingQueue<Record> queue = liveQueues[i];
        if (queue != null) {
          queue.offer(EOF_RECORD);
        }
      }
    }

    // Nobody wants the remaining splits any more, so interrupt them instead of politely
    // waiting five seconds for readers that may never come back.
    if (executor != null && !executor.isShutdown()) {
      executor.shutdownNow();
      try {
        if (!executor.awaitTermination(CLOSE_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
          LOG.warn("Download threads did not finish within " + CLOSE_AWAIT_TERMINATION_SECONDS
                   + "s after close; they are left as the only owner of their socket reader");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Clean up queues to help GC
    if (liveQueues != null) {
      for (int i = 0; i < liveQueues.length; i++) {
        liveQueues[i] = null;
      }
    }
  }

  /** The first failure a split reader reported, or null when the download was clean. */
  Throwable downloadFailure() {
    return error.get();
  }

  /**
   * True once every download thread of this iterator has finished. Only meaningful after
   * {@link #close()}, which waits for them.
   */
  boolean downloadThreadsTerminated() {
    return executor == null || executor.isTerminated();
  }

  public long getSplitSize() {
    return splitSize;
  }

  public int getPreloadSplitNum() {
    return preloadSplitNum;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public int getCurrentSplit() {
    return currentSplit;
  }

  public TableSchema getSchema() {
    if (isSelect) {
      return this.source.getSchema();
    } else {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("info", TypeInfoFactory.STRING));
      return schema;
    }
  }

  private Record getInfoRecord(Instance instance) throws OdpsException {
    Instance.InstanceResultModel.TaskResult taskResult = instance.getRawTaskResults().get(0);
    String result = taskResult.getResult().getString();
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("info", TypeInfoFactory.STRING));
    Record record = new ArrayRecord(schema);
    record.set(0, result);
    return record;
  }


  private static class EOFRecord implements Record {
    @Override public int getColumnCount() { return 0; }

    @Override
    public Column[] getColumns() {
      return new Column[0];
    }

    @Override
    public boolean isNull(int idx) {
      return false;
    }

    @Override
    public boolean isNull(String columnName) {
      return false;
    }

    @Override
    public void set(int idx, Object value) {

    }

    @Override public Object get(int i) { return null; }

    @Override
    public void set(String columnName, Object value) {

    }

    @Override
    public Object get(String columnName) {
      return null;
    }

    @Override
    public void setBigint(int idx, Long value) {

    }

    @Override
    public Long getBigint(int idx) {
      return 0L;
    }

    @Override
    public void setBigint(String columnName, Long value) {

    }

    @Override
    public Long getBigint(String columnName) {
      return 0L;
    }

    @Override
    public void setDouble(int idx, Double value) {

    }

    @Override
    public Double getDouble(int idx) {
      return 0.0;
    }

    @Override
    public void setDouble(String columnName, Double value) {

    }

    @Override
    public Double getDouble(String columnName) {
      return 0.0;
    }

    @Override
    public void setBoolean(int idx, Boolean value) {

    }

    @Override
    public Boolean getBoolean(int idx) {
      return null;
    }

    @Override
    public void setBoolean(String columnName, Boolean value) {

    }

    @Override
    public Boolean getBoolean(String columnName) {
      return null;
    }

    @Override
    public void setDatetime(int idx, Date value) {

    }

    @Override
    public Date getDatetime(int idx) {
      return null;
    }

    @Override
    public void setDatetime(String columnName, Date value) {

    }

    @Override
    public Date getDatetime(String columnName) {
      return null;
    }

    @Override
    public void setDecimal(int idx, BigDecimal value) {

    }

    @Override
    public BigDecimal getDecimal(int idx) {
      return null;
    }

    @Override
    public void setDecimal(String columnName, BigDecimal value) {

    }

    @Override
    public BigDecimal getDecimal(String columnName) {
      return null;
    }

    @Override
    public void setString(int idx, String value) {

    }

    @Override
    public String getString(int idx) {
      return "";
    }

    @Override
    public void setString(String columnName, String value) {

    }

    @Override
    public String getString(String columnName) {
      return "";
    }

    @Override
    public void setString(int idx, byte[] value) {

    }

    @Override
    public void setString(String columnName, byte[] value) {

    }

    @Override
    public byte[] getBytes(int idx) {
      return new byte[0];
    }

    @Override
    public byte[] getBytes(String columnName) {
      return new byte[0];
    }

    @Override
    public void set(Object[] values) {

    }

    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @Override
    public Record clone() {
      return null;
    }

    @Override
    public void clear() {

    }
    // Implement other Record methods as no-op
  }
}
