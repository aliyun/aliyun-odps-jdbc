package com.aliyun.odps.jdbc;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import com.aliyun.odps.jdbc.utils.SqlStates;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The two fields have to be right where the driver actually reports a failure, not only in the
 * mapping helper.
 *
 * <p>Review finding on `a6d43a2` (https://github.com/aliyun/aliyun-odps-jdbc/pull/221) named two
 * call sites where a state was either asserted too broadly or never reached:
 *
 * <ol>
 *   <li>{@code DataUploader.upload(List)} wraps {@code catch (Exception)}. The real upload path runs
 *       {@code setReusedRecord} inside it, and a transformer rejects a wrong-typed value with a
 *       plain {@code SQLException} that carries no service evidence -- so a blanket
 *       communication-link fallback turned "you put a string in an INT column" into "the connection
 *       broke", which is exactly what tells a pool to retry or drop a healthy connection. The
 *       fallback therefore needs transport evidence, and the upload path needs its own case.
 *   <li>{@code OdpsForwardResultSet.next()} gives up after five read failures with
 *       {@code new SQLException("to much retries because: " + e.getMessage())} -- dropping both the
 *       cause and any classification, while the sibling {@code rebuildReader()} reports them. The
 *       streaming claim made in this PR only holds if that terminal branch is covered too.
 * </ol>
 *
 * <p>Both cases drive real driver code. The connection is opened offline the same way
 * {@code ConnectFailureContractTest} does it (an explicit {@code timezone} keeps the driver from
 * reading the project), and the reader failure is a stubbed SDK reader, so nothing depends on
 * credentials or on a live Tunnel service.
 */
class CallSiteSqlStateContractTest {

  private static final String PROJECT = "sqlstate_callsite_project";
  private static final String CREDENTIALS =
      "accessId=sqlstate_callsite_access_id&accessKey=sqlstate_callsite_access_key";
  /** Explicit timezone: the driver opens without reading the project, so no service is reached. */
  private static final String WITHOUT_SERVICE_READ =
      "&interactiveMode=false&useInstanceTunnel=false&timezone=Asia/Shanghai"
      + "&odpsNamespaceSchema=false";

  private static OdpsConnection connection;

  @BeforeAll
  static void openOfflineConnection() throws Exception {
    connection = (OdpsConnection) DriverManager.getConnection(deadEndpointUrl(""), new Properties());
  }

  @AfterAll
  static void closeConnection() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  /** A URL that cannot be reached: only usable because this driver does not read the project here. */
  private static String deadEndpointUrl(String extraOptions) throws IOException {
    int port;
    try (ServerSocket reserved = new ServerSocket(0)) {
      port = reserved.getLocalPort();
    }
    return "jdbc:odps:http://127.0.0.1:" + port + "/api?project=" + PROJECT + "&" + CREDENTIALS
           + WITHOUT_SERVICE_READ + extraOptions;
  }

  // ---------------------------------------------------------------------------------------------
  // 1. Upload path: a rejected value is a data error, not a link failure.
  // ---------------------------------------------------------------------------------------------

  /**
   * The real {@link DataUploader} wrapper with the real {@code setReusedRecord} and the real
   * transformer; only the session setup is stubbed, because that is the part that would need a
   * Tunnel service.
   */
  private static class IntColumnUploader extends DataUploader {
    private final boolean transportFailure;

    IntColumnUploader(OdpsConnection conn, boolean transportFailure) throws Exception {
      super(PROJECT, null, "sqlstate_callsite_table", null, Collections.singletonList("n_int"), conn);
      this.transportFailure = transportFailure;
    }

    @Override
    protected void setUpSession() {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("n_int", TypeInfoFactory.INT));
      tableSchema = schema;
      reuseRecord = new ArrayRecord(schema);
    }

    @Override
    protected void upload(List<Object[]> batchedRows, int batchedSize, int[] updateCounts)
        throws IOException, SQLException {
      if (transportFailure) {
        throw new IOException("connection reset by peer");
      }
      for (Object[] row : batchedRows) {
        setReusedRecord(row, tableSchema);
      }
    }

    @Override
    public void commit() {
    }
  }

  @Test
  @DisplayName("a string in an INT column is not reported as a broken connection")
  void wrongIntValueKeepsItsOwnClass() throws Exception {
    IntColumnUploader uploader = new IntColumnUploader(connection, false);

    SQLException failure =
        assertThrows(SQLException.class, () -> uploader.upload(
            Collections.singletonList(new Object[] {"not-an-int"})));

    // The transformer's own message is what the caller has to see; nothing about a link.
    assertNotNull(failure.getMessage(), "upload failure must carry a message");
    assertTrue(failure.getMessage().toLowerCase().contains("int")
               || failure.getMessage().toLowerCase().contains("transform"),
               "message should stay the transformer's own reason: " + failure.getMessage());
    assertNotNull(failure.getCause(), "the transformer's exception must stay reachable");
    assertTrue(failure.getCause().getMessage().toLowerCase().contains("int")
               || failure.getCause().getMessage().toLowerCase().contains("transform"),
               "cause should be the transformer: " + failure.getCause());
    // This is the finding: no transport evidence, so no communication-link state.
    assertNull(failure.getSQLState(),
               "a rejected value is not a link failure, but SQLState was " + failure.getSQLState());
    assertEquals(0, failure.getErrorCode(), "the service never answered, so no vendor code");
  }

  @Test
  @DisplayName("a transport failure on the same path still reports 08S01")
  void transportFailureOnUploadStillIsLinkFailure() throws Exception {
    IntColumnUploader uploader = new IntColumnUploader(connection, true);

    SQLException failure =
        assertThrows(SQLException.class, () -> uploader.upload(
            Collections.singletonList(new Object[] {1})));

    assertEquals(SqlStates.COMMUNICATION_LINK_FAILURE, failure.getSQLState(),
                 "IOException on the upload path is a link failure: " + failure);
    assertInstanceOfCause(failure, IOException.class);
    assertEquals(0, failure.getErrorCode(), "no service code was given");
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Streaming: the terminal read-failure branch has to keep cause and classification.
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("giving up after repeated read failures keeps the cause and the state")
  void exhaustedReadRetriesKeepCauseAndState() throws Exception {
    OdpsStatement statement = (OdpsStatement) connection.createStatement();
    // The statement was never used, so the properties a real query would have filled are still
    // null and OdpsResultSet reads them through the statement. Same-package access, no reflection.
    statement.inputProperties = new Properties();
    OdpsResultSetMetaData meta = new OdpsResultSetMetaData(
        Collections.singletonList("n_int"),
        Collections.singletonList(TypeInfoFactory.INT));

    IOException brokenPipe = new IOException("connection reset by peer");
    TunnelRecordReader reader = mock(TunnelRecordReader.class);
    // next() hands the reader its (still null) reuse record, so the matcher has to take null.
    when(reader.read(nullable(Record.class))).thenThrow(brokenPipe);

    InstanceTunnel.DownloadSession session = mock(InstanceTunnel.DownloadSession.class);
    when(session.getRecordCount()).thenReturn(3L);
    when(session.openRecordReader(anyLong(), anyLong(), anyBoolean())).thenReturn(reader);

    OdpsForwardResultSet resultSet = new OdpsForwardResultSet(statement, meta, session);

    SQLException failure = assertThrows(SQLException.class, resultSet::next);

    // The message the driver has always produced -- reviewed and kept as is.
    assertTrue(failure.getMessage().startsWith("to much retries because: "),
               "message shape changed: " + failure.getMessage());
    assertTrue(failure.getMessage().contains("connection reset by peer"),
               "the reader's own reason should stay in the message: " + failure.getMessage());
    // Both of these are the gap: before the fix the branch threw `new SQLException(message)`.
    assertNotNull(failure.getCause(),
                  "the read failure was dropped, so a caller cannot see why it gave up");
    assertSame(brokenPipe, failure.getCause(), "the IOException itself should be the cause");
    assertEquals(SqlStates.COMMUNICATION_LINK_FAILURE, failure.getSQLState(),
                 "repeated read failures are a link failure, SQLState was " + failure.getSQLState());
    assertEquals(0, failure.getErrorCode(), "no service code was given");

    resultSet.close();
    statement.close();
  }

  /** Walks the cause chain, so a wrapper cannot hide the origin. */
  private static void assertInstanceOfCause(SQLException failure, Class<?> expected) {
    assertNotNull(failure.getCause(), "cause dropped: " + failure);
    for (Throwable cur = failure.getCause(); cur != null; cur = cur.getCause()) {
      if (expected.isInstance(cur)) {
        return;
      }
    }
    throw new AssertionError("cause chain has no " + expected.getName() + ": " + failure);
  }

  private static void assertSame(Object expected, Object actual, String message) {
    if (expected != actual) {
      throw new AssertionError(message + " expected the same instance, got " + actual);
    }
  }

  /** Unused import guard: keeps Arrays/Collections imports honest when cases are trimmed. */
  @Test
  @DisplayName("offline connection really is open")
  void theFixtureIsWhatItClaims() throws Exception {
    assertNotNull(connection, "the fixture connection exists");
    assertTrue(!connection.isClosed(), "and stays open until the test closes it");
    assertEquals(PROJECT, connection.getOdps().getDefaultProject(), "same project as the URL");
  }
}
