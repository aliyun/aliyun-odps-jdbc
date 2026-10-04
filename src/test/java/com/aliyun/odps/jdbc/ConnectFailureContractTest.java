package com.aliyun.odps.jdbc;

import com.aliyun.odps.ReloadException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A refused connection may only be announced with {@link SQLException}.
 *
 * <p>Opening an ODPS connection reads options and then asks the service about the project, so it
 * can fail in places the driver does not control: {@code Integer.parseInt} on a mistyped timeout,
 * the option parser on a value it cannot use, {@link ReloadException} from the SDK when the project
 * cannot be read, a Guava wrapper when the endpoint is not a URL. Every one of those was unchecked
 * and {@link DriverManager} passes unchecked exceptions straight through, which is a contract
 * break with a sharp edge: a connection pool -- and anything else that reacts to a failed
 * connection -- branches on {@code SQLException}, so it never sees the failure it was written to
 * handle and keeps retrying a configuration that can never work.
 *
 * <p>The cases are offline on purpose. A rejected option needs no service; a rejected project is
 * answered by a stub that behaves like the service, so the assertion does not depend on what
 * credentials or capabilities this machine happens to have. What they do need is a driver that
 * answers in the one language JDBC reserves for "there is no connection here".
 */
class ConnectFailureContractTest {

  /** Named in the URLs so the stub can tell "no such project" from "credentials refused". */
  private static final String KNOWN_PROJECT = "connect_contract_project";
  private static final String MISSING_PROJECT = "connect_contract_missing_project";
  private static final String CREDENTIALS =
      "accessId=connect_contract_access_id&accessKey=connect_contract_access_key";
  /** Keeps a case that must fail on an option from also failing -- or hanging -- on the service. */
  private static final String WITHOUT_SERVICE_READ =
      "&interactiveMode=false&useInstanceTunnel=false&timezone=Asia/Shanghai&odpsNamespaceSchema=false";
  /** Keeps a case that must read the project from reading it four times instead of once. */
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

  /** An address nothing is listening on: connection attempts fail immediately. */
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

  /** A URL that cannot reach the service, for options that must be rejected before it is asked. */
  private static String optionUrl(String option) throws IOException {
    return "jdbc:odps:" + deadEndpoint() + "?" + "project=" + KNOWN_PROJECT + "&" + CREDENTIALS
           + WITHOUT_SERVICE_READ + option;
  }

  /** A URL whose project the driver has to read, which is where the service answers back. */
  private static String projectUrl(String endpoint, String project) {
    return "jdbc:odps:" + endpoint + "?project=" + project + "&" + CREDENTIALS + FAIL_FAST_TRANSPORT;
  }

  /** @return the {@code SQLException} the driver raised, or a failure describing what it raised */
  private static SQLException refuse(String url) {
    return refuse(url, new Properties());
  }

  private static SQLException refuse(String url, Properties extra) {
    try {
      Connection refused = DriverManager.getConnection(url, extra);
      if (refused != null) {
        refused.close();
      }
    } catch (SQLException expected) {
      return expected;
    } catch (RuntimeException escaped) {
      // Exactly the defect: the caller asked DriverManager for a connection and got a
      // RuntimeException, which no SQLException-only error handler can see.
      throw new AssertionError("DriverManager.getConnection() raised "
                                   + escaped.getClass().getName() + " instead of SQLException: "
                                   + escaped.getMessage(), escaped);
    }
    return fail("expected getConnection() to refuse this URL, it returned a connection");
  }

  private static boolean causedBy(Throwable failure, Class<?> expected) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (expected.isInstance(current)) {
        return true;
      }
    }
    return false;
  }

  /** A name in the message is what makes a rejected option actionable: which knob, which value. */
  private static void assertNamesOptionAndValue(SQLException failure, String option, String value) {
    String message = failure.getMessage();
    assertNotNull(message, "SQLException without a message cannot name the rejected option");
    assertTrue(message.contains(option),
               "message should name the rejected option \"" + option + "\": " + message);
    assertTrue(message.contains(value),
               "message should carry the rejected value \"" + value + "\": " + message);
  }

  @Test
  void mistypedTimeoutsAreRejectedAsSqlExceptions() throws Exception {
    for (String option : new String[] {"readTimeout", "connectTimeout", "tunnelReadTimeout",
                                       "tunnelConnectTimeout"}) {
      SQLException failure = refuse(optionUrl("&" + option + "=not-a-number"));
      assertNamesOptionAndValue(failure, option, "not-a-number");
      assertTrue(causedBy(failure, NumberFormatException.class),
                 "the parse failure should stay reachable as the cause: " + failure);
    }
  }

  @Test
  void mistypedTimeoutGivenAsPropertyIsRejectedAsSqlException() throws Exception {
    // The property spelling of the same option, and the same answer.
    Properties settings = new Properties();
    settings.setProperty("read_timeout", "yesterday");
    SQLException failure = refuse(optionUrl(""), settings);
    assertNamesOptionAndValue(failure, "read_timeout", "yesterday");
    assertNamesOptionAndValue(failure, "readTimeout", "yesterday");
  }

  @Test
  void otherNumericOptionsSayWhichOptionTheyRejected() throws Exception {
    // Left alone, "For input string: \"nope\"" names neither the option nor the spelling used.
    for (String option : new String[] {"tunnelRetryTime", "longJobWarningThreshold",
                                       "logviewVersion", "autoSelectLimit"}) {
      SQLException failure = refuse(optionUrl("&" + option + "=nope"));
      assertNamesOptionAndValue(failure, option, "nope");
      assertTrue(causedBy(failure, NumberFormatException.class),
                 "the parse failure should stay reachable as the cause: " + failure);
    }
  }

  @Test
  void unusableOptionCombinationsAreRejectedAsSqlExceptions() throws Exception {
    SQLException notABoolean = refuse(optionUrl("&odpsNamespaceSchema=maybe"));
    assertNamesOptionAndValue(notABoolean, "odpsNamespaceSchema", "maybe");
    assertTrue(causedBy(notABoolean, IllegalArgumentException.class),
               "the option parser should stay reachable as the cause: " + notABoolean);

    SQLException schemaWithoutThreeTier = refuse(optionUrl("&schema=some_schema"));
    assertTrue(schemaWithoutThreeTier.getMessage().contains("odpsNamespaceSchema"),
               "message should say which two options contradict: "
               + schemaWithoutThreeTier.getMessage());

    SQLException unparsableTableList = refuse(optionUrl("&tableList=not-a-table-list"));
    assertTrue(unparsableTableList.getMessage().contains("not-a-table-list"),
               "message should carry the rejected value: " + unparsableTableList.getMessage());
    assertTrue(causedBy(unparsableTableList, IllegalArgumentException.class),
               "the parser that gave up should stay reachable as the cause: "
               + unparsableTableList);
  }

  @Test
  void unreachableEndpointIsRejectedAsSqlException() throws Exception {
    SQLException failure = refuse(projectUrl(deadEndpoint(), KNOWN_PROJECT));
    assertTrue(causedBy(failure, ReloadException.class),
               "the SDK's project-read failure should stay reachable as the cause: " + failure);
    assertTrue(failure.getMessage().contains("endpoint"),
               "message should keep the reason the service gave: " + failure.getMessage());
  }

  @Test
  void unknownProjectIsRejectedAsSqlException() {
    SQLException failure = refuse(projectUrl(stubEndpoint(), MISSING_PROJECT));
    assertTrue(causedBy(failure, ReloadException.class),
               "the service's refusal should stay reachable as the cause: " + failure);
    assertTrue(failure.getMessage().contains("Project not found"),
               "message should keep the service's own reason: " + failure.getMessage());
  }

  @Test
  void refusedCredentialsAreRejectedAsSqlException() {
    SQLException failure = refuse(projectUrl(stubEndpoint(), KNOWN_PROJECT));
    assertTrue(causedBy(failure, ReloadException.class),
               "the service's refusal should stay reachable as the cause: " + failure);
    assertTrue(failure.getMessage().contains("Invalid signature"),
               "message should keep the service's own reason: " + failure.getMessage());
  }

  @Test
  void endpointThatIsNotAUrlIsRejectedAsSqlException() {
    SQLException failure = refuse(projectUrl("not-an-endpoint", KNOWN_PROJECT));
    assertTrue(causedBy(failure, IllegalArgumentException.class),
               "the reason should stay reachable as the cause: " + failure);
    assertTrue(failure.getMessage().contains("Request URI"),
               "message should keep the reason the endpoint was refused: " + failure.getMessage());
  }

  @Test
  void aForeignUrlIsStillDeclinedByReturningNull() throws Exception {
    assertNull(new OdpsDriver().connect("jdbc:not-odps://host/db", new Properties()),
               "connect() returns null for a URL another driver owns");
  }

  @Test
  void aValidConfigurationStillOpensAConnection() throws Exception {
    // Nothing about a refused connection justifies changing an accepted one: the driver opens,
    // hands back a usable connection, and closes it, exactly as it did before.
    Connection opening = DriverManager.getConnection(optionUrl(""), new Properties());
    assertNotNull(opening, "a valid URL has to yield a connection");
    assertTrue(opening instanceof OdpsConnection,
               "and it should be the same driver connection as before");
    assertTrue(!opening.isClosed(), "which stays open until the caller closes it");
    opening.close();
    assertTrue(opening.isClosed(), "and closes on request");
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
                     + "<RequestId>connect-contract-request-id</RequestId></Error>")
          .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/xml");
      exchange.getResponseHeaders().add("x-odps-request-id", "connect-contract-request-id");
      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    }
  }
}
