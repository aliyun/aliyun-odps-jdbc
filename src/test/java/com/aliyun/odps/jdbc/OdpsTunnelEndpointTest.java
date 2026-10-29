package com.aliyun.odps.jdbc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises the real parallel download request, without cloud credentials or SQL submission. */
public class OdpsTunnelEndpointTest {
  private HttpServer routingServer;
  private HttpServer downloadServer;
  private final AtomicInteger routingRequests = new AtomicInteger();
  private final AtomicInteger downloadRequests = new AtomicInteger();

  @BeforeEach
  public void startServers() throws IOException {
    routingServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    routingServer.createContext("/", exchange -> {
      routingRequests.incrementAndGet();
      respond(exchange, 400,
          "<Error><Code>InvalidParameter</Code><Message>Unexpected automatic routing</Message></Error>");
    });
    routingServer.start();
    downloadServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    downloadServer.createContext("/", exchange -> {
      if (!"POST".equals(exchange.getRequestMethod())
          || !"/projects/tunnel_fixture/instances/fixture-instance".equals(
              exchange.getRequestURI().getPath())) {
        respond(exchange, 400,
            "<Error><Code>InvalidParameter</Code><Message>Unexpected download request</Message></Error>");
        return;
      }
      downloadRequests.incrementAndGet();
      respond(exchange, 200, "{\"DownloadID\":\"fixture-download\",\"Status\":\"NORMAL\","
          + "\"RecordCount\":0,\"Schema\":{\"columns\":[{\"name\":\"value\",\"type\":\"bigint\"}],"
          + "\"partitionKeys\":[]}}");
    });
    downloadServer.start();
  }

  @AfterEach
  public void stopServers() {
    downloadServer.stop(0);
    routingServer.stop(0);
  }

  @Test
  public void urlEndpointIsUsedByParallelStatementDownload() throws Exception {
    verifyParallelDownload(false, false, false);
  }

  @Test
  public void propertyEndpointOverridesUrlForPreparedDownload() throws Exception {
    verifyParallelDownload(true, false, true);
  }

  @Test
  public void asyncStatementAndPreparedDownloadsUseConfiguredEndpoint() throws Exception {
    verifyParallelDownload(false, true, false);
    verifyParallelDownload(false, true, true);
  }

  private void verifyParallelDownload(boolean propertyOverride, boolean async, boolean prepared)
      throws Exception {
    String downloadEndpoint = endpoint(downloadServer);
    String url = "jdbc:odps:" + endpoint(routingServer)
        + "?project=tunnel_fixture&timezone=UTC&odpsNamespaceSchema=false"
        + "&autoSelectLimit=1000000000&async=" + async
        + "&tunnelEndpoint=" + (propertyOverride ? endpoint(routingServer) : downloadEndpoint);
    Properties properties = new Properties();
    properties.setProperty("access_id", "fixture-id");
    properties.setProperty("access_key", "fixture-key");
    if (propertyOverride) {
      properties.setProperty("tunnel_endpoint", downloadEndpoint);
    }
    int previousDownloads = downloadRequests.get();
    try (OdpsConnection connection = new OdpsConnection(url, properties);
         OdpsStatement statement = prepared
             ? connection.prepareStatement("select 1") : connection.createStatement()) {
      assertFalse(connection.enableLimit());
      assertNull(connection.getSizeLimit());
      assertEquals(downloadEndpoint, connection.getTunnelEndpoint());
      // The executor owns a cloned Odps. The parallel iterator receives this original object.
      assertNull(connection.getOdps().getTunnelEndpoint());
      SQLExecutor executor = mock(SQLExecutor.class);
      when(executor.getExecuteMode()).thenReturn(ExecuteMode.OFFLINE);
      statement.sqlExecutor = executor;
      statement.executeInstance = mock(Instance.class);
      when(statement.executeInstance.getProject()).thenReturn("tunnel_fixture");
      when(statement.executeInstance.getId()).thenReturn("fixture-instance");
      statement.inputProperties = new Properties();
      if (!async) {
        statement.setResultSetInternal();
      }
      try (ResultSet rows = statement.getResultSet()) {
        assertEquals("value", rows.getMetaData().getColumnName(1));
        assertFalse(rows.next());
      }
      verify(statement.executeInstance).waitForSuccess();
      assertNull(connection.getOdps().getTunnelEndpoint(), "Do not mutate the shared Odps");
    }
    assertEquals(previousDownloads + 1, downloadRequests.get());
    assertEquals(0, routingRequests.get(), "The configured endpoint must bypass automatic routing");
  }

  private static String endpoint(HttpServer server) {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static void respond(HttpExchange exchange, int status, String text) throws IOException {
    byte[] body = text.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }
}
