package com.aliyun.odps.jdbc;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import org.junit.Test;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.*;
import static org.junit.Assert.*;

public class ResultSetLifecycleTest {
  private OdpsConnection connection() throws Exception {
    Properties p = new Properties();
    p.setProperty("access_id", "test-id");
    p.setProperty("access_key", "test-key");
    p.setProperty("project_name", "test_project");
    return new OdpsConnection("jdbc:odps:http://127.0.0.1:1?timezone=UTC"
        + "&odpsNamespaceSchema=false&tunnelEndpoint=http://127.0.0.1:1", p);
  }

  /** Real worker, closed through SDK ResultSet's AutoCloseable iterator contract. */
  private static class Download implements Iterator<Record>, AutoCloseable {
    final ExecutorService workers = Executors.newSingleThreadExecutor();
    final CountDownLatch started = new CountDownLatch(1);
    int closes;
    final TableSchema schema = new TableSchema();
    Download() throws Exception {
      schema.addColumn(new Column("n", OdpsType.BIGINT));
      workers.submit(() -> {
        started.countDown();
        try { new CountDownLatch(1).await(); }
        catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
      });
      assertTrue(started.await(2, TimeUnit.SECONDS));
    }
    public boolean hasNext() { return true; }
    public Record next() { ArrayRecord row = new ArrayRecord(schema); row.set(0, 42L); return row; }
    public void close() { closes++; workers.shutdownNow(); }
    com.aliyun.odps.data.ResultSet result() {
      return new com.aliyun.odps.data.ResultSet(this, schema, 1);
    }
    void assertReleased() throws Exception {
      assertEquals("underlying iterator must close exactly once", 1, closes);
      assertTrue("download worker must terminate", workers.awaitTermination(2, TimeUnit.SECONDS));
    }
  }

  @Test public void reusingStatementClosesUnclaimedDownload() throws Exception {
    try (OdpsConnection conn = connection(); Download download = new Download()) {
      OdpsStatement statement = conn.createStatement();
      statement.inputProperties = new Properties();
      statement.odpsResultSet = download.result();
      statement.beforeExecute();
      download.assertReleased();
      statement.close();
      download.assertReleased();
    }
  }

  @Test public void closingStatementClosesUnclaimedDownload() throws Exception {
    try (OdpsConnection conn = connection(); Download download = new Download()) {
      OdpsStatement statement = conn.createStatement();
      statement.inputProperties = new Properties();
      statement.odpsResultSet = download.result();
      statement.close();
      statement.close();
      download.assertReleased();
    }
  }

  @Test public void forwardResultOwnsDownloadUntilClosed() throws Exception {
    try (OdpsConnection conn = connection(); Download download = new Download()) {
      OdpsStatement statement = conn.createStatement();
      statement.inputProperties = new Properties();
      statement.odpsResultSet = download.result();
      java.sql.ResultSet rows = statement.getResultSet();
      assertEquals(0, download.closes);
      assertTrue(rows.next());
      assertEquals(42, rows.getLong(1));
      rows.close();
      statement.close();
      download.assertReleased();
    }
  }

  @Test public void reusingStatementClosesForwardDownload() throws Exception {
    try (OdpsConnection conn = connection(); Download download = new Download()) {
      OdpsStatement statement = conn.createStatement();
      statement.inputProperties = new Properties();
      statement.odpsResultSet = download.result();
      statement.getResultSet();
      statement.beforeExecute();
      download.assertReleased();
      statement.close();
      download.assertReleased();
    }
  }
  private void scrollConversion(boolean fail) throws Exception {
    try (OdpsConnection conn = connection(); Download download = new Download()) {
      OdpsStatement statement = new OdpsStatement(conn, true);
      statement.inputProperties = new Properties();
      statement.odpsResultSet = download.result();
      statement.sqlExecutor = (com.aliyun.odps.sqa.SQLExecutor) java.lang.reflect.Proxy.newProxyInstance(
          getClass().getClassLoader(), new Class<?>[]{com.aliyun.odps.sqa.SQLExecutor.class},
          (proxy, method, args) -> {
            switch (method.getName()) {
              case "getInstance": return conn.getOdps().instances().get("test-instance");
              case "getTaskName": return "test-task";
              case "getSubqueryId":
                if (fail) throw new IllegalStateException("conversion failure");
                return -1;
              case "getExecuteMode": return com.aliyun.odps.sqa.ExecuteMode.OFFLINE;
              default: return null;
            }
          });
      try {
        java.sql.ResultSet rows = statement.getResultSet();
        assertFalse(fail);
        assertTrue(rows instanceof OdpsScollResultSet);
        rows.close();
      } catch (IllegalStateException expected) {
        assertTrue(fail);
        assertEquals("conversion failure", expected.getMessage());
      }
      download.assertReleased();
      statement.close();
      download.assertReleased();
    }
  }

  @Test public void scrollConversionClosesOldDownload() throws Exception { scrollConversion(false); }
  @Test public void failedScrollConversionClosesOldDownload() throws Exception { scrollConversion(true); }

}
