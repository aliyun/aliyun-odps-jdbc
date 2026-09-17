package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.sqa.v2.MaxQAConnInfo;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import static org.junit.Assert.*;

/** Uses a local metadata endpoint; no live credentials or user tables. */
public class MaxQAFallbackBackportTest {
  private static class TestConnection extends OdpsConnection {
    TestConnection(String url, Properties properties) throws SQLException { super(url, properties); }
    @Override public boolean checkIfEnableMaxQA(String quota) {
      return quota != null && quota.startsWith("maxqa");
    }
  }

  private void verify(String options, String expectedQuota, boolean disabled) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/connection/mcqa", exchange -> {
      String query = exchange.getRequestURI().getQuery();
      String quota = query.contains("maxqa_second") ? "maxqa_second" : "maxqa_first";
      byte[] body = ("{\"quotaNickName\":\"" + quota
          + "\",\"connInfo\":\"test-connection\",\"regionId\":\"test-region\"}")
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (java.io.OutputStream out = exchange.getResponseBody()) { out.write(body); }
    });
    java.util.concurrent.atomic.AtomicReference<String> header = new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicInteger submissions = new java.util.concurrent.atomic.AtomicInteger();
    server.createContext("/mcqa/projects/test_project/instances", exchange -> {
      header.set(exchange.getRequestHeaders().getFirst("x-odps-fallback-infos"));
      submissions.incrementAndGet();
      while (exchange.getRequestBody().read() != -1) { }
      byte[] body = "<Error><Code>InvalidParameter</Code><Message>test submission captured</Message></Error>"
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(400, body.length);
      try (java.io.OutputStream out = exchange.getResponseBody()) { out.write(body); }
    });
    server.start();
    Properties props = new Properties();
    props.setProperty("access_id", "test-id");
    props.setProperty("access_key", "test-key");
    props.setProperty("project_name", "test_project");
    String url = "jdbc:odps:http://127.0.0.1:" + server.getAddress().getPort()
        + "?quotaName=maxqa_first&timezone=UTC&odpsNamespaceSchema=false" + options;
    try (OdpsConnection conn = new TestConnection(url, props)) {
      assertEquals(ExecuteMode.INTERACTIVE_V2, conn.getExecutorBuilder().getExecuteMode());
      assertInfo(conn.getExecutorBuilder().getMaxQAConnInfo(), "maxqa_first", expectedQuota, disabled);
      try (OdpsStatement statement = conn.createStatement()) {
        Properties settings = new Properties();
        settings.setProperty("odps.task.wlm.quota", "maxqa_second");
        statement.processSetClauseExtra(settings);
        assertNotSame(conn.getExecutor(), statement.sqlExecutor);
        try { statement.sqlExecutor.run("select 1", new java.util.HashMap<String, String>()); }
        catch (com.aliyun.odps.OdpsException expected) { assertEquals("test submission captured", expected.getMessage()); }
        assertEquals(1, submissions.get());
        if (disabled) assertNull(header.get());
        else {
          com.google.gson.JsonObject payload = new com.google.gson.JsonParser().parse(header.get()).getAsJsonObject();
          assertEquals("true", payload.get("Fallback").getAsString());
          if (expectedQuota != null) assertEquals(expectedQuota, payload.get("FallbackQuota").getAsString());
          else assertFalse(payload.has("FallbackQuota"));
        }
      }
      SQLExecutorBuilder switched = conn.getExecutorBuilder().clone();
      conn.configureMaxQA(switched, "maxqa_second", true);
      switched.build(); // SDK resolves new connection metadata and must retain FallbackInfo.
      assertInfo(switched.getMaxQAConnInfo(), "maxqa_second", expectedQuota, disabled);
      assertInfo(conn.getExecutorBuilder().getMaxQAConnInfo(), "maxqa_first", expectedQuota, disabled);
      conn.configureMaxQA(switched, "default", false);
      assertNull(switched.getMaxQAConnInfo());
      assertEquals(ExecuteMode.OFFLINE, switched.getExecuteMode());
      switched.tunnelEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
      switched.build();
    } finally { server.stop(0); }
  }

  private void assertInfo(MaxQAConnInfo info, String quota, String fallbackQuota, boolean disabled) {
    assertNotNull(info);
    assertEquals(quota, info.getQuotaName());
    assertEquals("test-connection", info.getConnInfo());
    if (disabled) assertNull(info.getFallbackInfo());
    else {
      assertNotNull(info.getFallbackInfo());
      assertEquals(fallbackQuota, info.getFallbackInfo().getFallbackQuota());
    }
  }

  @Test public void defaultFallbackSurvivesMetadataReload() throws Exception { verify("", null, false); }
  @Test public void customFallbackQuotaSurvivesMetadataReload() throws Exception {
    verify("&fallbackQuota=offline_quota", "offline_quota", false);
  }
  @Test public void disableWinsOverFallbackQuota() throws Exception {
    verify("&disableFallback=true&fallbackQuota=offline_quota", null, true);
  }
  @Test public void emptyFallbackQuotaUsesServerDefault() throws Exception { verify("&fallbackQuota=", null, false); }
  @Test public void propertiesAliasesRemainSupported() {
    Properties props = new Properties();
    props.setProperty("disable_fallback", "true");
    props.setProperty("fallback_quota", "offline_quota");
    ConnectionResource resource = new ConnectionResource("jdbc:odps:http://localhost", props);
    assertTrue(resource.isDisableFallback());
    assertEquals("offline_quota", resource.getFallbackQuota());
  }
}
