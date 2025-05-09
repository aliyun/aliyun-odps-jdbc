package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.SQLExecutorConstants;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Use sharded concurrent download mode to download sample data.
 * Variable parameter split size, and number of preload splits.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InstanceDataIterator implements Iterator<Record>, AutoCloseable {

  private static final Record EOF_RECORD = new EOFRecord();
  private boolean isSelect = true;

  private ExecutorService executor;
  private int splitNum;
  private BlockingQueue<Record>[] queues;
  private AtomicReference<Throwable> error = new AtomicReference<>();
  private long offset;
  private long recordCount;
  private int preloadSplitNum;
  private long splitSize;
  private InstanceTunnel.DownloadSession downloadSession;
  private int threadNum;

  private int currentSplit = 0;

  private Record currentRecord;

  public InstanceDataIterator(Odps odps, Instance instance, long offset, Long readCount, long splitSize, int preloadSplitNum, int threadNum)
      throws OdpsException {
    try {
      this.downloadSession = new InstanceTunnel(odps).createDownloadSession(instance.getProject(), instance.getId(), false);
    } catch (TunnelException e) {
      if (e.getErrorCode().equals(SQLExecutorConstants.sessionNotSelectException)
          || e.getErrorMsg().contains(SQLExecutorConstants.sessionNotSelectMessage)) {
        isSelect = false;
        currentRecord = getInfoRecord(instance);
        return;
      }
      throw e;
    }
    this.offset = offset;
    this.recordCount = (readCount == null || readCount < 0) ? downloadSession.getRecordCount() - offset : Math.min(readCount, (downloadSession.getRecordCount() - offset));
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


  private int computeSplitNum(long splitSize, long recordCount) {
    return (int) ((recordCount + splitSize - 1) / splitSize);
  }

  private synchronized void submitNextSplit(int splitIndex) {
    if (splitIndex >= splitNum) return;

    long start = offset + splitIndex * splitSize;
    long count = Math.min(splitSize, recordCount - (splitIndex * splitSize));

    queues[splitIndex] = new LinkedBlockingQueue<>();
    executor.submit(() -> {
      TunnelRecordReader reader = null;
      try {
        reader = downloadSession.openRecordReader(start, count);
        Record record;
        while ((record = reader.read()) != null) {
          queues[splitIndex].put(record);
        }
        queues[splitIndex].put(EOF_RECORD);
      } catch (Throwable t) {
        error.compareAndSet(null, t);
        queues[splitIndex].offer(EOF_RECORD); // Ensure queue is marked as complete
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ignored) {
          }
        }
      }
    });
  }

  private synchronized boolean hasNextInternal() {
    checkError();
    if (currentSplit >= splitNum) {
      currentRecord = EOF_RECORD;
      return false;
    }
    BlockingQueue<Record> currentQueue = queues[currentSplit];
    try {
      Record record = currentQueue.take();
      if (record == EOF_RECORD) {
        queues[currentSplit] = null;
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

  @Override
  public void close() {
    executor.shutdownNow();
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
      return this.downloadSession.getSchema();
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
    // Implement other Record methods as no-op
  }
}
