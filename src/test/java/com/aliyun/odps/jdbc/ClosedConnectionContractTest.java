package com.aliyun.odps.jdbc;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;

import com.aliyun.odps.jdbc.utils.SqlStates;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Contract regression for using a Connection after close().
 *
 * <p>Defect this test guards: {@code prepareStatement(String)} ({@code OdpsConnection.java:531})
 * and {@code setAutoCommit(boolean)} ({@code :634}) never consulted {@code checkClosed()}, while
 * the other connection-level entry points in the same file do ({@code prepareStatement(String,int,
 * int)} at {@code :572}, {@code getMetaData()} at {@code :684}, {@code createStatement()} at
 * {@code :781}, {@code createStatement(int,int)} at {@code :803}, {@code getSchema()} at
 * {@code :929}). Whether "this connection is already closed" reached the caller therefore depended
 * on which overload the caller happened to pick:
 *
 * <ul>
 *   <li>{@code prepareStatement("select 1")} handed back a live, unclosed {@link Statement} bound
 *       to a connection whose {@code isClosed} was already {@code true}. The failure surfaced
 *       later, inside execution, as whatever the service said about the request rather than as
 *       "the connection is closed"; and {@code stmtHandles.add(stmt)} at {@code :538} registered a
 *       handle on a connection that had already run its cleanup pass, so nothing would close it.
 *   <li>{@code setAutoCommit(true)} returned normally. That is the call a pool makes to reset a
 *       connection when it is returned; answering "fine" tells the pool the physical connection is
 *       still usable.
 * </ul>
 *
 * <p>The spec is explicit for both methods: they "throw an SQLException when called on a closed
 * connection". What is raised here is the exception this driver already raises for the guarded
 * overloads -- same type, same message, same SQLState coming out of {@code checkClosed()} -- so
 * the change removes a divergence instead of inventing a convention. The state value belongs to
 * the SQLState mapping already merged in this driver; nothing here picks a new one.
 *
 * <p>Runs fully offline, with the same fixture as {@link ClosedStatementContractTest}: the URL
 * points at a loopback port that is closed again immediately, and {@code timezone},
 * {@code odpsNamespaceSchema=false} and {@code useInstanceTunnel=false} keep the constructor from
 * making any remote call, so a real driver object exists without credentials or a server.
 * {@code close()} does no network work outside interactive mode, so the closed state is reachable
 * offline as well.
 */
class ClosedConnectionContractTest {

  private static final String CLOSED_MESSAGE = "the connection has already been closed";

  private Connection connection;

  @BeforeEach
  void openOfflineConnection() throws Exception {
    int port;
    try (ServerSocket probe = new ServerSocket(0)) {
      port = probe.getLocalPort();
    }
    String url = "jdbc:odps:http://127.0.0.1:" + port + "/api"
        + "?timezone=Asia/Shanghai&odpsNamespaceSchema=false&useInstanceTunnel=false";
    Properties properties = new Properties();
    properties.setProperty("access_id", "placeholder");
    properties.setProperty("access_key", "placeholder");
    properties.setProperty("project_name", "placeholder");
    properties.setProperty("log_level", "OFF");
    connection = DriverManager.getConnection(url, properties);
    assertFalse(connection.isClosed(), "the offline fixture must produce an open connection");
  }

  @AfterEach
  void closeConnection() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  private interface Invocation {
    void run() throws Exception;
  }

  /**
   * Asserts the call raises the closed-connection SQLException this driver already reports.
   *
   * <p>Message and SQLState are the values {@code checkClosed()} emits, which is exactly what the
   * two unguarded overloads fail to reach; a silent return or an escaped NPE both fail loudly.
   */
  private void assertClosedConnectionSqlException(String label, Invocation invocation) {
    try {
      invocation.run();
      fail(label + ": expected SQLException on a closed connection, but nothing was thrown");
    } catch (SQLException e) {
      assertEquals(CLOSED_MESSAGE, e.getMessage(), label + ": unexpected message: " + e);
      assertEquals(SqlStates.CONNECTION_NOT_IN_USE, e.getSQLState(),
          label + ": unexpected SQLState: " + e.getSQLState());
    } catch (Exception e) {
      fail(label + ": expected SQLException, got " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  @Test
  @DisplayName("closed Connection: prepareStatement(String) throws instead of returning a handle")
  void prepareStatementOnClosedConnectionThrows() throws Exception {
    connection.close();
    assertTrue(connection.isClosed());

    // Pre-fix this returned an OdpsPreparedStatement whose isClosed() was false.
    assertClosedConnectionSqlException("Connection.prepareStatement(String)",
        () -> connection.prepareStatement("select 1"));
  }

  @Test
  @DisplayName("closed Connection: setAutoCommit(true) throws instead of silently succeeding")
  void setAutoCommitOnClosedConnectionThrows() throws Exception {
    connection.close();

    // The value a pool resets to on return; pre-fix it was a silent no-op.
    assertClosedConnectionSqlException("Connection.setAutoCommit(true)",
        () -> connection.setAutoCommit(true));
  }

  @Test
  @DisplayName("closed Connection: setAutoCommit(false) reports the closed connection, not the feature")
  void setAutoCommitFalseOnClosedConnectionReportsClosedFirst() throws Exception {
    connection.close();

    // Deliberate ordering: a closed connection cannot be configured at all, so the closed check
    // precedes the "disabling autocommit is not supported" rejection. Both are SQLExceptions, and
    // the rejection on an open connection is unchanged (see openConnectionKeepsPriorBehaviour).
    assertClosedConnectionSqlException("Connection.setAutoCommit(false)",
        () -> connection.setAutoCommit(false));
  }

  @Test
  @DisplayName("the entry points that already guard define the shape the two fixed ones match")
  void alreadyGuardedEntryPointsKeepTheirShape() throws Exception {
    connection.close();

    assertClosedConnectionSqlException("Connection.createStatement()",
        () -> connection.createStatement());
    assertClosedConnectionSqlException("Connection.createStatement(int,int)",
        () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertClosedConnectionSqlException("Connection.prepareStatement(String,int,int)",
        () -> connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY));
    assertClosedConnectionSqlException("Connection.getMetaData()", () -> connection.getMetaData());
    assertClosedConnectionSqlException("Connection.getSchema()", () -> connection.getSchema());
  }

  @Test
  @DisplayName("open Connection keeps its behaviour: prepare works, setAutoCommit(false) stays a feature rejection")
  void openConnectionKeepsPriorBehaviour() throws Exception {
    PreparedStatement prepared = connection.prepareStatement("select 1 where 1 = ?");
    assertNotNull(prepared, "an open connection must still hand out a statement");
    assertFalse(prepared.isClosed());
    prepared.setString(1, "x");
    prepared.close();

    Statement plain = connection.createStatement();
    plain.setMaxRows(10);
    assertEquals(10, plain.getMaxRows());
    plain.close();

    assertTrue(connection.getAutoCommit());
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setAutoCommit(false));
    assertTrue(connection.getAutoCommit(), "a rejected setAutoCommit() must not flip the flag");
  }

  @Test
  @DisplayName("close() stays idempotent, and every guarded call still reports closed afterwards")
  void closeRemainsIdempotent() throws Exception {
    Statement statement = connection.createStatement();
    statement.close();
    connection.close();
    connection.close();
    assertTrue(connection.isClosed());

    assertClosedConnectionSqlException("Connection.prepareStatement(String)",
        () -> connection.prepareStatement("select 1"));
    assertClosedConnectionSqlException("Connection.setAutoCommit(true)",
        () -> connection.setAutoCommit(true));
    assertClosedConnectionSqlException("Connection.createStatement()",
        () -> connection.createStatement());
  }
}
