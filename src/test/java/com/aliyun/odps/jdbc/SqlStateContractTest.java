package com.aliyun.odps.jdbc;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.jdbc.utils.SqlStates;
import com.aliyun.odps.tunnel.TunnelException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A failed call has to answer in the two machine-readable fields JDBC gave it.
 *
 * <p>{@link SQLException#getSQLState()} and {@link SQLException#getErrorCode()} are how a caller
 * that never saw this driver -- a connection pool deciding between retry and fail-fast, a BI tool
 * or an ETL framework mapping a failure -- classifies what happened. Before this, every failure
 * the driver reported from a service error carried {@code null} and {@code 0}: the only readable
 * identity was the {@code ODPS-0130071}-style code inside the message text, which each caller
 * would have to parse for itself. aliyun/aliyun-odps-jdbc#67 asks for the standard fields.
 *
 * <p>Two rules hold the mapping honest:
 *
 * <ul>
 *   <li>A code is only reported where the driver has evidence for it. The values are the published
 *       SQL-standard / JDBC classes (08/28/3D/3F/42 families), never a private table dressed up as
 *       one. Where nothing is known -- a {@code NullPointerException} in the driver, a service code
 *       whose family we have not observed -- {@code SQLState} stays {@code null} rather than
 *       claiming {@code HY000}. A fabricated state is worse than none: it teaches callers to branch
 *       on a value that means whatever the driver felt like.
 *   <li>Only fields that were empty are filled. The message text, the exception type and the cause
 *       chain are untouched, so an application that already reads the message keeps working.
 * </ul>
 *
 * <p>The service-shaped cases are driven through a stub that answers like MaxCompute does (the
 * same harness {@link ConnectFailureContractTest} uses), so the assertion is about the driver's
 * public behaviour and not about what this machine's credentials allow.
 */
class SqlStateContractTest {

  private static final String KNOWN_PROJECT = "sqlstate_contract_project";
  private static final String MISSING_PROJECT = "sqlstate_contract_missing_project";
  private static final String CREDENTIALS =
      "accessId=sqlstate_contract_access_id&accessKey=sqlstate_contract_access_key";
  private static final String WITHOUT_SERVICE_READ =
      "&interactiveMode=false&useInstanceTunnel=false&timezone=Asia/Shanghai"
      + "&odpsNamespaceSchema=false";
  private static final String FAIL_FAST_TRANSPORT =
      "&interactiveMode=false&useInstanceTunnel=false&retryTime=1&connectTimeout=1&readTimeout=1";

  private static HttpServer service;

  @BeforeAll
  static void startStubService() throws IOException {
    service = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    service.createContext("/", new RejectingHandler());
    service.start();
  }

  @AfterAll
  static void stopStubService() {
    if (service != null) {
      service.stop(0);
    }
  }

  private static String deadEndpoint() throws IOException {
    int port;
    try (ServerSocket reserved = new ServerSocket(0)) {
      port = reserved.getLocalPort();
    }
    return "http://127.0.0.1:" + port + "/api";
  }

  private static String stubEndpoint() {
    return "http://127.0.0.1:" + service.getAddress().getPort() + "/api";
  }

  private static String optionUrl(String option) throws IOException {
    return "jdbc:odps:" + deadEndpoint() + "?" + "project=" + KNOWN_PROJECT + "&" + CREDENTIALS
           + WITHOUT_SERVICE_READ + option;
  }

  private static String projectUrl(String endpoint, String project) {
    return "jdbc:odps:" + endpoint + "?project=" + project + "&" + CREDENTIALS + FAIL_FAST_TRANSPORT;
  }

  /** @return the SQLException the driver raised while opening, or a failure describing what it raised */
  private static SQLException refuseToOpen(String url) throws IOException {
    try {
      Connection opened = DriverManager.getConnection(url, new Properties());
      if (opened != null) {
        opened.close();
      }
    } catch (SQLException expected) {
      return expected;
    }
    throw new AssertionError("expected getConnection() to refuse this URL: " + url);
  }

  // ---------------------------------------------------------------------------------------------
  // 1. The mapping, one case per row of the table in the README.
  // ---------------------------------------------------------------------------------------------

  private static void assertStateAndCode(String description, Throwable cause, String expectedState,
                                         int expectedCode) {
    assertEquals(expectedState, SqlStates.resolveSqlState(cause), description + ": SQLState");
    assertEquals(expectedCode, SqlStates.resolveErrorCode(cause), description + ": vendor code");
  }

  @Test
  @DisplayName("SQL compile and semantic failures are class 42")
  void compileAndSemanticFailuresAreClass42() {
    assertStateAndCode("semantic analysis",
                       new OdpsException("ODPS-0130071:[1,8] Semantic analysis exception - column"
                                         + " `ds` cannot be resolved"),
                       SqlStates.SYNTAX_ERROR_OR_ACCESS_RULE, 130071);
    assertStateAndCode("parse error",
                       new OdpsException("ODPS-0130161:[1,1] Parse exception - invalid token"
                                         + " 'SELEC'"),
                       SqlStates.SYNTAX_ERROR_OR_ACCESS_RULE, 130161);
  }

  @Test
  @DisplayName("a missing table, project or schema is named by its own class")
  void missingObjectsAreNamed() {
    assertStateAndCode("table not found in a statement",
                       new OdpsException("ODPS-0130131:[1,15] Table not found - table"
                                         + " `p.default.t` does not exist"),
                       SqlStates.TABLE_NOT_FOUND, 130131);
    assertStateAndCode("the SDK's own missing-table type",
                       new NoSuchObjectException("Table 't' does not exist"),
                       SqlStates.TABLE_NOT_FOUND, 0);
    assertStateAndCode("missing project",
                       odpsWithCode("NoSuchObject", "ODPS-0420111:Project not found - project 'p'"
                                                       + " does not exist"),
                       SqlStates.INVALID_CATALOG_NAME, 420111);
    assertStateAndCode("missing schema",
                       odpsWithCode("NoSuchSchema", "ODPS-0420111:Schema not found - schema 's'"
                                                    + " does not exist"),
                       SqlStates.INVALID_SCHEMA_NAME, 420111);
    assertStateAndCode("schema layer the project does not have",
                       new OdpsException("ODPS-0110061: Failed to run ddltask - Invalid database"
                                         + " operations on two-tier model"),
                       SqlStates.INVALID_SCHEMA_NAME, 110061);
  }

  @Test
  @DisplayName("refused identity and refused privilege are different answers")
  void authorizationIsSplit() {
    // The account itself is not accepted: class 28, the caller must fix the credentials.
    assertStateAndCode("bad signature",
                       odpsWithCode("Unauthorized",
                                    "ODPS-0410042:Invalid signature value - User"
                                    + " signature does not match"),
                       SqlStates.INVALID_AUTHORIZATION, 410042);
    assertStateAndCode("invalid token, named only by its REST code",
                       odpsWithCode("InvalidAccount",
                                    "InvalidStsToken - the security token you provided is"
                                    + " invalid"),
                       SqlStates.INVALID_AUTHORIZATION, 0);
    // The account is accepted but may not touch this object: class 42, privilege rule.
    assertStateAndCode("authorization exception",
                       new OdpsException("ODPS-0130013:Authorization exception - Authorization"
                                         + " Failed [4019], You have NO privilege 'odps:Select' on"
                                         + " {acs:odps:*:projects/p/tables/t}"),
                       SqlStates.INSUFFICIENT_PRIVILEGE, 130013);
    assertStateAndCode("tunnel refuses the upload",
                       tunnelWithError("NoPermission",
                                       "NoPermission: you have no permission to access the table"),
                       SqlStates.INSUFFICIENT_PRIVILEGE, 0);
  }

  @Test
  @DisplayName("session (MaxQA) failure flags map to the class the flag documents")
  void sessionFlagsMap() {
    assertStateAndCode("unsupported in session",
                       new OdpsException("ODPS-1850001 - 'use project' is not supported in odps"
                                         + " jdbc for now"),
                       SqlStates.FEATURE_NOT_SUPPORTED, 1850001);
    assertStateAndCode("session query timeout",
                       new OdpsException("ODPS-1860011: query timeout"),
                       SqlStates.QUERY_TIMEOUT_EXPIRED, 1860011);
    assertStateAndCode("session access denied",
                       new OdpsException("ODPS-1820002: no public service for this project"),
                       SqlStates.INSUFFICIENT_PRIVILEGE, 1820002);
  }

  @Test
  @DisplayName("a failure with no evidence gets no code, not HY000")
  void unknownFailuresStayUnclassified() {
    assertStateAndCode("an NPE in the driver", new NullPointerException("boom"), null, 0);
    assertStateAndCode("a code family nobody has observed",
                       new OdpsException("ODPS-9999999: something happened"), null, 9999999);
    assertStateAndCode("an unchecked wrapper with no service text",
                       new ReloadException(new IllegalStateException("no project here")), null, 0);
    // Transport failures carry no service identity; the caller's context decides, so resolve()
    // alone must not guess a class for them.
    assertStateAndCode("connection reset", new SocketTimeoutException("read timed out"), null, 0);
    assertStateAndCode("no such host", new UnknownHostException("endpoint.invalid"), null, 0);
  }

  @Test
  @DisplayName("the evidence is found through the wrapper, not just on the surface")
  void evidenceIsSearchedThroughTheCauseChain() {
    // This is the shape the SDK actually hands the driver: ReloadException(OdpsException(...)).
    Exception wrapped = new ReloadException(
        new OdpsException("ODPS-0420111:Project not found - project 'p' does not exist"));
    assertStateAndCode("wrapped project read failure", wrapped, SqlStates.INVALID_CATALOG_NAME,
                       420111);
    TunnelException overIo = tunnelWithError("NoSuchObject",
                                             "ODPS-0420111:Project not found - nope");
    assertStateAndCode("tunnel object missing",
                       new IOException("stream failed", overIo), SqlStates.INVALID_CATALOG_NAME,
                       420111);
    // A cycle in the cause chain must not spin forever.
    SQLException loopHead = new SQLException("a");
    RuntimeException loopTail = new RuntimeException("b", loopHead);
    loopHead.initCause(loopTail);
    assertNull(SqlStates.resolveSqlState(loopHead), "a cyclic chain still gets an answer");
  }

  // ---------------------------------------------------------------------------------------------
  // 2. The context each call site adds: where the driver was standing when it failed.
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("an opening failure that never reached a service is 08001")
  void openingWithoutAnAnswerIsUnableToConnect() throws Exception {
    SQLException failure = refuseToOpen(projectUrl(deadEndpoint(), KNOWN_PROJECT));
    assertEquals(SqlStates.UNABLE_TO_CONNECT, failure.getSQLState(),
                 "unreachable endpoint: " + failure);
    assertEquals(0, failure.getErrorCode(), "no service code was ever given");

    // A rejected option is also "this URL cannot open a connection", from the driver's side.
    SQLException rejectedOption = refuseToOpen(optionUrl("&readTimeout=not-a-number"));
    assertEquals(SqlStates.UNABLE_TO_CONNECT, rejectedOption.getSQLState(),
                 "mistyped option: " + rejectedOption);
  }

  @Test
  @DisplayName("an opening failure the service answered is classified by that answer")
  void openingWithAnAnswerIsClassifiedByTheAnswer() throws Exception {
    SQLException missingProject = refuseToOpen(projectUrl(stubEndpoint(), MISSING_PROJECT));
    assertEquals(SqlStates.INVALID_CATALOG_NAME, missingProject.getSQLState(),
                 "project not found: " + missingProject);
    assertEquals(420111, missingProject.getErrorCode(), "project not found: " + missingProject);

    SQLException refusedCredentials = refuseToOpen(projectUrl(stubEndpoint(), KNOWN_PROJECT));
    assertEquals(SqlStates.INVALID_AUTHORIZATION, refusedCredentials.getSQLState(),
                 "bad signature: " + refusedCredentials);
    assertEquals(410042, refusedCredentials.getErrorCode(), "bad signature: " + refusedCredentials);
  }

  @Test
  @DisplayName("a read failure while streaming a result is a link failure")
  void streamingReadFailureIsLinkFailure() {
    SQLException built = SqlStates.sqlException("Failed to read the next record",
                                                new IOException("connection reset"),
                                                SqlStates.COMMUNICATION_LINK_FAILURE);
    assertEquals(SqlStates.COMMUNICATION_LINK_FAILURE, built.getSQLState(), "stream read");
    assertEquals(0, built.getErrorCode(), "stream read");

    // ...unless the service did answer underneath the IOException, which outranks the fallback.
    SQLException answered = SqlStates.sqlException(
        "Failed to read the next record",
        new IOException("stream failed",
                        new OdpsException("ODPS-0130131:Table not found - table `t` gone")),
        SqlStates.COMMUNICATION_LINK_FAILURE);
    assertEquals(SqlStates.TABLE_NOT_FOUND, answered.getSQLState(), "service answer outranks");
    assertEquals(130131, answered.getErrorCode(), "service answer outranks");
  }

  @Test
  @DisplayName("the exception the driver builds keeps message, cause and type")
  void theWrapperStaysTheSameShape() {
    OdpsException cause = new OdpsException("ODPS-0130071: Semantic analysis exception");
    SQLException built = SqlStates.sqlException("execute sql [ select 1 ] failed. boom", cause);
    assertEquals("execute sql [ select 1 ] failed. boom", built.getMessage(), "message");
    assertEquals(cause, built.getCause(), "cause");
    assertEquals(SQLException.class, built.getClass(), "type");
    assertEquals(SqlStates.SYNTAX_ERROR_OR_ACCESS_RULE, built.getSQLState(), "state");
    assertEquals(130071, built.getErrorCode(), "vendor code");
  }

  @Test
  @DisplayName("a known code with no defensible class fills only the vendor code")
  void vendorCodeWithoutAFabricatedState() {
    SQLException built = SqlStates.sqlException(
        "run failed", new OdpsException("ODPS-9999999: something happened"));
    assertNull(built.getSQLState(), "the driver must not invent a class");
    assertEquals(9999999, built.getErrorCode(), "the code the service gave is still reportable");
  }

  /** The shape the SDK builds when the REST body carries a {@code <Code>} element. */
  private static OdpsException odpsWithCode(String code, String message) {
    return new OdpsException(code, message);
  }

  private static TunnelException tunnelWithError(String code, String message) {
    TunnelException e = new TunnelException(message);
    e.setErrorCode(code);
    return e;
  }

  /** Answers like the service: one project does not exist, the credentials are not accepted. */
  private static class RejectingHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      boolean projectMissing = exchange.getRequestURI().getPath().contains(MISSING_PROJECT);
      int status = projectMissing ? 404 : 403;
      String code = projectMissing ? "NoSuchObject" : "Unauthorized";
      String reason = projectMissing
                      ? "ODPS-0420111:Project not found - project '" + MISSING_PROJECT
                            + "' does not exist"
                      : "ODPS-0410042:Invalid signature value - User signature does not match";
      byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                     + "<Error><Code>" + code + "</Code><Message>" + reason + "</Message>"
                     + "<RequestId>sqlstate-contract-request-id</RequestId></Error>")
          .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/xml");
      exchange.getResponseHeaders().add("x-odps-request-id", "sqlstate-contract-request-id");
      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    }
  }
}
