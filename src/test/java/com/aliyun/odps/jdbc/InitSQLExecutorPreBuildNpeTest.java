package com.aliyun.odps.jdbc;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.sqa.ExecuteMode;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression for the pre-build dereference of the {@code executor} field in {@link
 * OdpsConnection#initSQLExecutor}. The MaxQA branch used to evaluate {@code
 * executor.getInstance().getId()} inside a log statement, while that field is only assigned by
 * {@code builder.build()} further down and starts as {@code null}. Every connection that entered
 * the branch therefore died with a bare {@link NullPointerException} in {@code OdpsConnection.<init>}
 * before any SQL was submitted. The case names and the assertions below are scoped to that one
 * defect: nothing here claims that a MaxQA session can be attached or that a query can run.
 *
 * <p>Two independent signals separate "reached the builder" from "died before it", measured against
 * a loopback stub rather than a refused port so that no retry/backoff timing is involved (2026-11-28,
 * JDK 1.8.0_502, both arms ~0.4 s):
 * <ul>
 *   <li>the base throws {@code NullPointerException} with the top frame {@code initSQLExecutor};</li>
 *   <li>the base issues exactly one {@code /connection/mcqa} lookup - the driver's own, at line 336 -
 *       and never gets further, while the fixed code issues a second one from inside the SDK builder
 *       and surfaces {@code SQLException} caused by {@code OdpsException}.</li>
 * </ul>
 *
 * <p>Notes on the fixture, because two of them were corrections from review:
 * {@code retryTime=0} is parsed but only applied when positive ({@code OdpsConnection} guards it with
 * {@code if (retryTime > 0)}), so it does not suppress SDK retries and is deliberately not relied on
 * here; and the quota lookup at line 336 runs before executor initialization even with
 * {@code timezone}/{@code odpsNamespaceSchema=false}/{@code useInstanceTunnel=false}, which is exactly
 * why the request count above is a usable stage marker. Test method names keep the {@code test}
 * prefix because this maintenance line pins no surefire version - Maven falls back to 2.12.4, which
 * reaches Jupiter classes through the JUnit3/POJO checker and only runs {@code test*} methods; with
 * Jupiter-style names {@code mvn test} reports "Tests run: 0" together with BUILD SUCCESS.
 */
public class InitSQLExecutorPreBuildNpeTest {

  private static final String MCQA_PATH = "/connection/mcqa";

  private static Properties credentials() {
    Properties info = new Properties();
    info.setProperty("accessId", "dummy_access_id");
    info.setProperty("accessKey", "dummy_access_key");
    return info;
  }

  /** A stub that answers every request with a non-retryable 400 and records the request line. */
  private static final class Stub implements AutoCloseable {
    final HttpServer server;
    final List<String> requests = new CopyOnWriteArrayList<>();

    Stub() throws Exception {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", exchange -> {
        String uri = String.valueOf(exchange.getRequestURI());
        requests.add(exchange.getRequestMethod() + " " + uri);
        byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>ODPS-0410061</Code>"
            + "<Message>stub: routed quota is not a MaxQA quota</Message>"
            + "<RequestId>STUB0000000000000000000000000001</RequestId></Error>").getBytes("UTF-8");
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        exchange.sendResponseHeaders(400, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      });
      server.start();
    }

    int mcqaLookups() {
      int n = 0;
      for (String r : requests) {
        if (r.contains(MCQA_PATH)) {
          n++;
        }
      }
      return n;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static String url(int port, String interactiveMode) {
    return "jdbc:odps:http://127.0.0.1:" + port + "/api?project=dummy_project"
        + "&accessId=dummy_access_id&accessKey=dummy_access_key&interactiveMode=" + interactiveMode
        + "&timezone=Asia%2FShanghai&odpsNamespaceSchema=false&useInstanceTunnel=false";
  }

  /**
   * Branch entry, asserted directly: {@code interactiveMode=maxqa} maps to {@code INTERACTIVE_V2} and
   * enters {@code if (enableMaxQA || interactiveMode == INTERACTIVE_V2)} on its own, without depending
   * on the quota lookup. {@code mcqa}/{@code true} select {@code INTERACTIVE}, {@code false}/{@code
   * offline} select {@code OFFLINE}.
   */
  @Test
  public void testMaxqaKeywordSelectsTheInteractiveV2Branch() {
    try {
      Stub stub = new Stub();
      String u = url(stub.server.getAddress().getPort(), "maxqa");
      assertEquals(ExecuteMode.INTERACTIVE_V2, new ConnectionResource(u, credentials()).isInteractiveMode());
      assertEquals(ExecuteMode.INTERACTIVE,
          new ConnectionResource(url(1, "true"), credentials()).isInteractiveMode());
      assertEquals(ExecuteMode.INTERACTIVE,
          new ConnectionResource(url(1, "mcqa"), credentials()).isInteractiveMode());
      assertEquals(ExecuteMode.OFFLINE,
          new ConnectionResource(url(1, "false"), credentials()).isInteractiveMode());
      assertEquals(0, stub.requests.size(), "no connection attempt is expected for these assertions");
    } catch (Exception e) {
      fail("stub setup failed: " + e);
    }
  }

  /**
   * The regression itself: entering the branch must not dereference an unassigned executor, and the
   * failure must be located after {@code builder.build()} was reached rather than before it.
   */
  @Test
  public void testEnteringTheMaxQABranchReachesExecutorConstructionWithoutNpe() {
    try (Stub stub = new Stub()) {
      try (Connection ignored = DriverManager.getConnection(
          url(stub.server.getAddress().getPort(), "maxqa"), credentials())) {
        // Constructing at all already proves there is no pre-build dereference left.
      } catch (NullPointerException npe) {
        fail("executor was dereferenced before builder.build(): " + firstOdpsFrame(npe)
            + " with " + stub.mcqaLookups() + " quota lookup(s) recorded (message=" + npe.getMessage() + ")");
      } catch (SQLException e) {
        assertFalse(hasNullPointerCause(e),
            "NullPointerException must not appear in the chain: " + firstOdpsFrame(e));
        assertTrue(e.getCause() instanceof OdpsException,
            "expected a service-layer failure after the builder is entered, got " + e.getClass().getName()
                + " / cause " + e.getCause());
        // The driver's own lookup (OdpsConnection line 336) plus the SDK builder's second one:
        // dying before build() can only ever produce the first.
        assertTrue(stub.mcqaLookups() >= 2,
            "expected the builder's own quota lookup to run as well, recorded "
                + stub.mcqaLookups() + " lookup(s): " + stub.requests);
      }
    } catch (Exception e) {
      fail("stub setup failed: " + e);
    }
  }

  /**
   * Control: {@code interactiveMode=false} never enters that branch. With the three offline switches
   * the constructor performs no request against the stub at all, which is what makes the lookup
   * counts above attributable to the branch and not to connection setup.
   */
  @Test
  public void testOfflineModeControlEntersNoBranchAndMakesNoLookup() {
    try (Stub stub = new Stub()) {
      try (Connection c = DriverManager.getConnection(
          url(stub.server.getAddress().getPort(), "false"), credentials())) {
        assertTrue(c.isValid(5), "control connection should be usable");
      }
      assertEquals(Collections.emptyList(), stub.requests,
          "offline mode must not perform the MaxQA lookup that the branch under test follows");
    } catch (Exception e) {
      fail("control case failed unexpectedly: " + e);
    }
  }

  private static boolean hasNullPointerCause(Throwable t) {
    for (Throwable r = t; r != null; r = r.getCause()) {
      if (r instanceof NullPointerException) {
        return true;
      }
    }
    return false;
  }

  /** Keeps a failure readable: the first frame inside this project. */
  private static String firstOdpsFrame(Throwable t) {
    for (StackTraceElement s : t.getStackTrace()) {
      if (s.getClassName().startsWith("com.aliyun.odps")) {
        return s.getClassName() + "." + s.getMethodName() + ":" + s.getLineNumber();
      }
    }
    return "n/a";
  }
}
