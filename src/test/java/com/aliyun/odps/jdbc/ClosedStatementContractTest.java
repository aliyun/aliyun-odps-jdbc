package com.aliyun.odps.jdbc;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Contract regression for using a Statement / PreparedStatement after close().
 *
 * <p>Defect this test guards: every public call on a closed statement dereferenced the
 * nulled {@code connHandle} before the closed check and leaked
 * {@link NullPointerException} instead of the {@link SQLException} the JDBC spec requires
 * ("this method is called on a closed Statement"). It was reported by the JDK 8/11/17/21
 * consumer matrix probe; behaviour was identical on all four JVMs.
 *
 * <p>Runs fully offline: the connection URL points at a loopback port that is closed again
 * immediately, and the three properties (timezone, odpsNamespaceSchema=false,
 * useInstanceTunnel=false) keep {@code OdpsConnection}'s constructor from any remote call,
 * so a real driver object can be produced without credentials or a server.
 */
class ClosedStatementContractTest {

  private static final String CLOSED_MESSAGE = "The statement has been closed";

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
   * Asserts the call raises a spec-compliant SQLException with SQLState 55000 and the
   * canonical closed message; an escaped NPE (the pre-fix behaviour) fails loudly.
   */
  private static void assertClosedSqlException(String label, Invocation invocation) {
    try {
      invocation.run();
      fail(label + ": expected SQLException on a closed statement, but nothing was thrown");
    } catch (SQLException e) {
      assertEquals(CLOSED_MESSAGE, e.getMessage(), label + ": unexpected message: " + e);
      assertEquals(OdpsStatement.SQLSTATE_OBJECT_CLOSED, e.getSQLState(),
          label + ": unexpected SQLState: " + e.getSQLState());
    } catch (NullPointerException e) {
      throw new AssertionError(label + ": NullPointerException escaped instead of SQLException",
          e);
    } catch (Exception e) {
      fail(label + ": expected SQLException, got " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  private Map<String, Invocation> closedPlainStatementCalls() throws SQLException {
    Statement statement = connection.createStatement();
    statement.close();
    Map<String, Invocation> calls = new LinkedHashMap<>();
    calls.put("executeQuery(String)", () -> statement.executeQuery("select 1"));
    calls.put("executeUpdate(String)", () -> statement.executeUpdate("select 1"));
    calls.put("execute(String)", () -> statement.execute("select 1"));
    calls.put("setQueryTimeout(int)", () -> statement.setQueryTimeout(1));
    calls.put("getQueryTimeout()", statement::getQueryTimeout);
    calls.put("getResultSet()", statement::getResultSet);
    calls.put("getMoreResults()", statement::getMoreResults);
    calls.put("getUpdateCount()", statement::getUpdateCount);
    calls.put("cancel()", statement::cancel);
    calls.put("getConnection()", statement::getConnection);
    calls.put("getMaxRows()", statement::getMaxRows);
    calls.put("setMaxRows(int)", () -> statement.setMaxRows(1));
    calls.put("getWarnings()", statement::getWarnings);
    calls.put("clearWarnings()", statement::clearWarnings);
    calls.put("setEscapeProcessing(boolean)", () -> statement.setEscapeProcessing(true));
    calls.put("setFetchDirection(int)",
        () -> statement.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD));
    calls.put("setFetchSize(int)", () -> statement.setFetchSize(10));
    calls.put("getFetchSize()", statement::getFetchSize);
    calls.put("getFetchDirection()", statement::getFetchDirection);
    calls.put("isCloseOnCompletion()", statement::isCloseOnCompletion);
    calls.put("isPoolable()", statement::isPoolable);
    calls.put("getResultSetType()", statement::getResultSetType);
    calls.put("hasResultSet(String) [deprecated]",
        () -> ((OdpsStatement) statement).hasResultSet("select 1"));
    return calls;
  }

  @Test
  @DisplayName("closed Statement: every spec-covered method throws SQLException(55000)")
  void closedStatementMethodsThrowSqlException() throws SQLException {
    List<String> checked = new ArrayList<>();
    for (Map.Entry<String, Invocation> call : closedPlainStatementCalls().entrySet()) {
      assertClosedSqlException("Statement." + call.getKey(), call.getValue());
      checked.add(call.getKey());
    }
    assertTrue(checked.size() >= 20, "guard matrix unexpectedly shrank: " + checked.size());
  }

  @Test
  @DisplayName("closed PreparedStatement: execute*/set*/batch methods throw SQLException(55000)")
  void closedPreparedStatementMethodsThrowSqlException() throws SQLException {
    PreparedStatement prepared = connection.prepareStatement("select 1 where 1 = ?");
    prepared.close();
    assertClosedSqlException("PreparedStatement.executeQuery()", prepared::executeQuery);
    assertClosedSqlException("PreparedStatement.executeUpdate()", prepared::executeUpdate);
    assertClosedSqlException("PreparedStatement.execute()", prepared::execute);
    assertClosedSqlException("PreparedStatement.setString(int,String)",
        () -> prepared.setString(1, "x"));
    assertClosedSqlException("PreparedStatement.setInt(int,int)", () -> prepared.setInt(1, 1));
    assertClosedSqlException("PreparedStatement.setObject(int,Object)",
        () -> prepared.setObject(1, "x"));
    assertClosedSqlException("PreparedStatement.setNull(int,int)",
        () -> prepared.setNull(1, java.sql.Types.INTEGER));
    assertClosedSqlException("PreparedStatement.clearParameters()", prepared::clearParameters);
    assertClosedSqlException("PreparedStatement.addBatch()", prepared::addBatch);
    assertClosedSqlException("PreparedStatement.clearBatch()", prepared::clearBatch);
    assertClosedSqlException("PreparedStatement.executeBatch()", prepared::executeBatch);
    // inherited surface must stay closed-safe as well (NPE sites came through delegation)
    assertClosedSqlException("PreparedStatement.executeQuery inherited guard",
        () -> prepared.executeQuery("select 1"));
  }

  @Test
  @DisplayName("double close() stays a no-op and isClosed() keeps answering")
  void closeRemainsIdempotent() throws SQLException {
    Statement statement = connection.createStatement();
    statement.close();
    statement.close();
    assertTrue(statement.isClosed());

    PreparedStatement prepared = connection.prepareStatement("select 1");
    prepared.close();
    prepared.close();
    assertTrue(prepared.isClosed());
  }

  @Test
  @DisplayName("methods that report feature-not-supported keep doing so (they are SQLExceptions)")
  void featureNotSupportedBehaviourUnchanged() throws SQLException {
    Statement statement = connection.createStatement();
    statement.close();
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.addBatch("select 1"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeBatch());
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.getGeneratedKeys());
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setMaxFieldSize(10));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.getMaxFieldSize());
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setCursorName("n"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setPoolable(true));
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> statement.executeUpdate("select 1", Statement.RETURN_GENERATED_KEYS));
  }

  @Test
  @DisplayName("live statements are unaffected: setters still work while open")
  void openStatementKeepsPriorBehaviour() throws SQLException {
    Statement statement = connection.createStatement();
    statement.setMaxRows(10);
    assertEquals(10, statement.getMaxRows());
    statement.setFetchSize(12345);
    assertEquals(12345, statement.getFetchSize());
    assertEquals(java.sql.ResultSet.FETCH_FORWARD == statement.getFetchDirection()
                 || java.sql.ResultSet.FETCH_UNKNOWN == statement.getFetchDirection(), true);
    statement.close();
  }
}
