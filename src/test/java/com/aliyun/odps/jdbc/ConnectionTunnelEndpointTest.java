package com.aliyun.odps.jdbc;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.QueryInfo;
import com.aliyun.odps.sqa.v2.MaxQAConnInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.Checksum;
import com.google.protobuf.CodedOutputStream;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real SDK session/read/write/commit requests must stay on the configured loopback server. */
public class ConnectionTunnelEndpointTest {
  private HttpServer router;
  private Endpoint first;
  private Endpoint second;
  private final CopyOnWriteArrayList<String> unexpected = new CopyOnWriteArrayList<>();
  private volatile boolean allowAutomaticRouting;
  private final AtomicInteger automaticRoutes = new AtomicInteger();
  private byte[] record;
  private byte[] recordWithSchema;
  private static SSLContext serverTls;
  private static final String SCHEMA = "{\"columns\":[{\"name\":\"n\",\"type\":\"bigint\"}],\"partitionKeys\":[]}";

  @BeforeAll
  public static void prepareLocalTls() throws Exception {
    Path dir = Files.createTempDirectory("jdbc-endpoint-tls-");
    Path store = dir.resolve("fixture.p12");
    Path keytool = java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "keytool");
    if (!Files.isExecutable(keytool)) {
      keytool = java.nio.file.Paths.get(System.getProperty("java.home"), "..", "bin", "keytool");
    }
    Process process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "fixture",
        "-keyalg", "RSA", "-keysize", "2048", "-validity", "1", "-storetype", "PKCS12",
        "-keystore", store.toString(), "-storepass", "fixture-password", "-keypass", "fixture-password",
        "-dname", "CN=localhost", "-ext", "SAN=IP:127.0.0.1,DNS:localhost")
        .redirectErrorStream(true).redirectOutput(dir.resolve("keytool.log").toFile()).start();
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Local TLS fixture generation timed out");
    assertEquals(0, process.exitValue(), "Local TLS fixture generation failed");
    char[] password = "fixture-password".toCharArray();
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (java.io.InputStream in = Files.newInputStream(store)) { keys.load(in, password); }
    KeyManagerFactory managers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    managers.init(keys, password);
    serverTls = SSLContext.getInstance("TLS");
    serverTls.init(managers.getKeyManagers(), null, null);
    Files.deleteIfExists(store);
    Files.deleteIfExists(dir.resolve("keytool.log"));
    Files.deleteIfExists(dir);
  }

  @BeforeEach
  public void start() throws Exception {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("n", TypeInfoFactory.BIGINT));
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    try (ProtobufRecordStreamWriter writer = new ProtobufRecordStreamWriter(schema, data,
        new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, -1, 0))) {
      ArrayRecord row = new ArrayRecord(schema);
      row.setBigint(0, 42L);
      writer.write(row);
    }
    record = data.toByteArray();
    ByteArrayOutputStream prefix = new ByteArrayOutputStream();
    CodedOutputStream schemaStream = CodedOutputStream.newInstance(prefix);
    Checksum schemaChecksum = new Checksum();
    byte[] schemaBytes = SCHEMA.getBytes(StandardCharsets.UTF_8);
    schemaChecksum.update(1);
    schemaChecksum.update(schemaBytes, 0, schemaBytes.length);
    schemaStream.writeString(1, SCHEMA);
    // SDK's schema-in-stream protocol uses a different end tag from ordinary records.
    schemaStream.writeUInt32(33553920, (int) schemaChecksum.getValue());
    schemaStream.flush();
    prefix.write(record);
    recordWithSchema = prefix.toByteArray();
    router = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    router.createContext("/", exchange -> {
      if (exchange.getRequestURI().getPath().equals("/projects/endpoint_fixture/logs")) {
        // SDK telemetry remains a service request, independent of Tunnel routing.
        respond(exchange, 200, "{}".getBytes(StandardCharsets.UTF_8));
        return;
      }
      if (allowAutomaticRouting && exchange.getRequestURI().getPath().endsWith("/tunnel")) {
        automaticRoutes.incrementAndGet();
        respond(exchange, 200, ("127.0.0.1:" + first.server.getAddress().getPort()).getBytes(StandardCharsets.UTF_8));
        return;
      }
      unexpected.add(exchange.getRequestURI().toString());
      respond(exchange, 400, "<Error><Code>InvalidParameter</Code><Message>Unexpected routing or service request</Message></Error>".getBytes(StandardCharsets.UTF_8));
    });
    router.start();
    first = new Endpoint();
    second = new Endpoint();
  }

  @AfterEach
  public void stop() {
    first.server.stop(0);
    second.server.stop(0);
    router.stop(0);
    assertTrue(unexpected.isEmpty(), "Requests escaped the configured endpoint: " + unexpected);
  }

  @Test
  public void downloadsWithAndWithoutAutoLimitAcrossAllEntryPoints() throws Exception {
    for (boolean async : new boolean[] {false, true}) {
      for (boolean prepared : new boolean[] {false, true}) {
        for (String options : Arrays.asList("", "&enableLimit=false", "&autoSelectLimit=1000000000")) {
          download(options + "&async=" + async, new Properties(), prepared, false, ExecuteMode.OFFLINE);
        }
      }
    }
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("POST")));
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("GET")));
  }

  @Test
  public void scrollableDownloadsUseTheConnectionEndpoint() throws Exception {
    for (boolean prepared : new boolean[] {false, true}) {
      download("", new Properties(), prepared, true, ExecuteMode.OFFLINE);
      download("&enableLimit=false", new Properties(), prepared, true, ExecuteMode.OFFLINE);
    }
  }

  @Test
  public void blankPropertiesDoNotMaskTheUrlAndNonBlankPropertiesStillWin() throws Exception {
    for (String endpoint : new String[] {"", "   ", first.url(), second.url()}) {
      Properties properties = new Properties();
      properties.setProperty("tunnel_endpoint", endpoint);
      int firstBefore = first.requests.size();
      int secondBefore = second.requests.size();
      download("&enableLimit=false", properties, false, false, ExecuteMode.OFFLINE);
      if (endpoint.equals(second.url())) {
        assertEquals(firstBefore, first.requests.size());
        assertTrue(second.requests.size() > secondBefore);
      } else {
        assertTrue(first.requests.size() > firstBefore);
        assertEquals(secondBefore, second.requests.size());
      }
    }
  }

  @Test
  public void maxQaAndItsOfflineResultPathKeepTheEndpoint() throws Exception {
    download("&interactiveMode=MaxQA", new Properties(), false, false, ExecuteMode.INTERACTIVE_V2);
    download("&interactiveMode=MaxQA&enableLimit=false", new Properties(), false, false, ExecuteMode.OFFLINE);
  }

  @Test
  public void mcQaDirectDownloadKeepsTheEndpoint() throws Exception {
    download("", new Properties(), false, false, ExecuteMode.INTERACTIVE);
  }

  @Test
  public void basicUploadCreatesWritesReloadsAndCommitsOnTheEndpoint() throws Exception {
    try (OdpsConnection connection = connection("", new Properties())) {
      BasicTableUploader uploader = new BasicTableUploader("endpoint_fixture", null, "fixture_table",
          null, Collections.singletonList("n"), connection);
      assertArrayEquals(new int[] {1}, uploader.upload(Collections.singletonList(new Object[] {42L})));
      uploader.commit();
    }
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("PUT")));
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("GET") && s.contains("uploadid=")));
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("POST") && s.contains("uploadid=")));
  }

  @Test
  public void upsertCreatesWritesAndCommitsOnTheEndpoint() throws Exception {
    try (OdpsConnection connection = connection("", new Properties())) {
      upsert(connection);
    }
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("PUT") && s.contains("/upserts")));
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("POST") && s.contains("upsertid=")));
  }

  @Test
  public void uploadSessionConsumesConnectionTunnelTimeouts() throws Exception {
    try (OdpsConnection connection =
        connection("&tunnelConnectTimeout=7&tunnelReadTimeout=30", new Properties())) {
      com.aliyun.odps.tunnel.TableTunnel.UploadSession session = uploadSession(connection);
      assertEquals(7, session.getConfig().getSocketConnectTimeout());
      assertEquals(30, session.getConfig().getSocketTimeout());
      // The SDK derives the session's REST client from that configuration, so these two reads are
      // the consumption proof, not just a copy of what was written.
      com.aliyun.odps.rest.RestClient client =
          (com.aliyun.odps.rest.RestClient) read(session, "tunnelServiceClient");
      assertEquals(7, client.getConnectTimeout(), "upload session connect timeout");
      assertEquals(30, client.getReadTimeout(), "upload session read timeout");
    }
  }

  @Test
  public void upsertSessionConsumesConnectionTunnelTimeouts() throws Exception {
    try (OdpsConnection connection =
        connection("&tunnelConnectTimeout=7&tunnelReadTimeout=30", new Properties())) {
      com.aliyun.odps.tunnel.impl.UpsertSessionImpl session = upsertSession(connection);
      // Seconds in, milliseconds out at the netty layer (README states the conversion point).
      assertEquals(7000L, session.getConnectTimeout(), "upsert session connect timeout");
      assertEquals(30000L, session.getReadTimeout(), "upsert session read timeout");
    }
  }

  @Test
  public void unsetTunnelTimeoutsKeepSdkDefaultsOnWriteSessions() throws Exception {
    assertEquals(180, com.aliyun.odps.commons.GeneralConfiguration.DEFAULT_SOCKET_CONNECT_TIMEOUT);
    assertEquals(300, com.aliyun.odps.commons.GeneralConfiguration.DEFAULT_SOCKET_TIMEOUT);
    for (String options : new String[] {"", "&tunnelConnectTimeout=-1&tunnelReadTimeout=-1"}) {
      try (OdpsConnection connection = connection(options, new Properties())) {
        com.aliyun.odps.tunnel.TableTunnel.UploadSession upload = uploadSession(connection);
        assertEquals(180, upload.getConfig().getSocketConnectTimeout(), "upload " + options);
        assertEquals(300, upload.getConfig().getSocketTimeout(), "upload " + options);
        com.aliyun.odps.tunnel.impl.UpsertSessionImpl upsert = upsertSession(connection);
        assertEquals(180000L, upsert.getConnectTimeout(), "upsert " + options);
        assertEquals(300000L, upsert.getReadTimeout(), "upsert " + options);
      }
    }
  }

  @Test
  public void tunnelTimeoutsAreGatedPerOptionAndStayLocalToTheConnection() throws Exception {
    try (OdpsConnection connection = connection("&tunnelReadTimeout=30", new Properties())) {
      com.aliyun.odps.tunnel.TableTunnel.UploadSession upload = uploadSession(connection);
      assertEquals(180, upload.getConfig().getSocketConnectTimeout(), "unset option is not written");
      assertEquals(30, upload.getConfig().getSocketTimeout());
    }
    // 0 is a configured value: it is handed over, it does not fall back to the SDK default.
    try (OdpsConnection connection =
        connection("&tunnelConnectTimeout=0&tunnelReadTimeout=0", new Properties())) {
      com.aliyun.odps.tunnel.TableTunnel.UploadSession upload = uploadSession(connection);
      assertEquals(0, upload.getConfig().getSocketConnectTimeout());
      assertEquals(0, upload.getConfig().getSocketTimeout());
      com.aliyun.odps.tunnel.impl.UpsertSessionImpl upsert = upsertSession(connection);
      assertEquals(0L, upsert.getConnectTimeout());
      assertEquals(0L, upsert.getReadTimeout());
    }
    // A later connection without the options must not inherit the previous client's values.
    try (OdpsConnection connection = connection("", new Properties())) {
      com.aliyun.odps.tunnel.TableTunnel.UploadSession upload = uploadSession(connection);
      assertEquals(180, upload.getConfig().getSocketConnectTimeout());
      assertEquals(300, upload.getConfig().getSocketTimeout());
    }
  }

  private com.aliyun.odps.tunnel.TableTunnel.UploadSession uploadSession(OdpsConnection connection)
      throws Exception {
    BasicTableUploader uploader = new BasicTableUploader("endpoint_fixture", null, "fixture_table",
        null, Collections.singletonList("n"), connection);
    return (com.aliyun.odps.tunnel.TableTunnel.UploadSession) read(uploader, "uploadSession");
  }

  private com.aliyun.odps.tunnel.impl.UpsertSessionImpl upsertSession(OdpsConnection connection)
      throws Exception {
    AcidTableUploader uploader = new AcidTableUploader("endpoint_fixture", null, "fixture_table",
        null, Collections.singletonList("n"), connection);
    return (com.aliyun.odps.tunnel.impl.UpsertSessionImpl) read(uploader, "upsertSession");
  }

  /** Reads a driver- or SDK-private field, walking up the class hierarchy. */
  private static Object read(Object target, String name) throws Exception {
    for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
      try {
        Field field = c.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException expected) {
        // declared by a supertype
      }
    }
    throw new NoSuchFieldException(name + " on " + target.getClass().getName());
  }

  @Test
  public void httpsEndpointWorksForDownloadsUploadsAndUpsertsWithHttpService() throws Exception {
    Endpoint secure = new Endpoint(true);
    try {
      Properties properties = new Properties();
      properties.setProperty("tunnel_endpoint", secure.url());
      download("&enableLimit=false", properties, false, false, ExecuteMode.OFFLINE);
      try (OdpsConnection connection = connection("", properties)) {
        BasicTableUploader uploader = new BasicTableUploader("endpoint_fixture", null, "fixture_table",
            null, Collections.singletonList("n"), connection);
        uploader.upload(Collections.singletonList(new Object[] {42L}));
        uploader.commit();
        upsert(connection);
      }
      assertTrue(first.requests.isEmpty());
      assertTrue(secure.requests.stream().anyMatch(s -> s.startsWith("PUT") && s.contains("/upserts")));
    } finally {
      secure.server.stop(0);
    }
  }

  @Test
  public void httpUpsertEndpointDoesNotInheritHttpsFromTheServiceEndpoint() throws Exception {
    try (OdpsConnection connection = new OdpsConnection(url("").replace("jdbc:odps:http:", "jdbc:odps:https:"), credentials())) {
      upsert(connection);
    }
    assertTrue(first.requests.stream().anyMatch(s -> s.startsWith("PUT") && s.contains("/upserts")));
  }

  @Test
  public void downloadReadRetryKeepsTheSameEndpoint() throws Exception {
    first.corruptReads.set(1);
    download("", new Properties(), false, false, ExecuteMode.OFFLINE);
    assertTrue(first.requests.stream().filter(s -> s.startsWith("GET")).count() >= 2);
  }

  @Test
  public void blobStorageApiFallbackKeepsTheEndpoint() throws Exception {
    first.blobFallback = true;
    assertThrows(Exception.class, () -> download("", new Properties(), false, false, ExecuteMode.OFFLINE));
    assertTrue(first.requests.stream().anyMatch(s -> s.contains("Action=InstanceCreateReadSession")), first.requests.toString());
  }

  private static void upsert(OdpsConnection connection) throws Exception {
    AcidTableUploader uploader = new AcidTableUploader("endpoint_fixture", null, "fixture_table",
        null, Collections.singletonList("n"), connection);
    try {
      assertArrayEquals(new int[] {1}, uploader.upload(Collections.singletonList(new Object[] {42L})));
      uploader.commit();
    } finally {
      Field f = AcidTableUploader.class.getDeclaredField("upsertSession");
      f.setAccessible(true);
      com.aliyun.odps.tunnel.TableTunnel.UpsertSession session =
          (com.aliyun.odps.tunnel.TableTunnel.UpsertSession) f.get(uploader);
      if (session != null) { session.close(); }
    }
  }

  @Test
  public void failedExplicitEndpointDoesNotFallBackToRouting() throws Exception {
    first.fail = true;
    for (String options : new String[] {"", "&enableLimit=false"}) {
      assertThrows(Exception.class, () -> download(options, new Properties(), false, false, ExecuteMode.OFFLINE));
    }
    assertFalse(first.requests.isEmpty());
  }

  @Test
  public void absentEndpointStillUsesAutomaticRouting() throws Exception {
    allowAutomaticRouting = true;
    String noEndpoint = url("").replace("&tunnelEndpoint=" + first.url(), "");
    try (OdpsConnection connection = new OdpsConnection(noEndpoint, credentials())) {
      assertNull(connection.getTunnelEndpoint());
      com.aliyun.odps.tunnel.InstanceTunnel.DownloadSession session =
          new com.aliyun.odps.tunnel.InstanceTunnel(connection.getOdps())
              .createDownloadSession("endpoint_fixture", "fixture-instance");
      try (com.aliyun.odps.tunnel.io.TunnelRecordReader reader = session.openRecordReader(0, 1)) {
        assertEquals(42L, reader.read().getBigint(0));
      }
      BasicTableUploader uploader = new BasicTableUploader("endpoint_fixture", null, "fixture_table",
          null, Collections.singletonList("n"), connection);
      uploader.upload(Collections.singletonList(new Object[] {42L}));
      uploader.commit();
      upsert(connection);
    }
    assertTrue(automaticRoutes.get() > 0);
  }

  @Test
  public void invalidEndpointFailsBeforeAnyNetworkRequest() throws Exception {
    for (String endpoint : new String[] {"dt.example.invalid", "file:///tmp/test", "https://user:secret@dt.example.invalid", "https://dt.example.invalid?q=1"}) {
      Properties properties = credentials();
      properties.setProperty("tunnel_endpoint", endpoint);
      assertThrows(SQLException.class, () -> new OdpsDriver().connect(url(""), properties));
    }
    assertTrue(first.requests.isEmpty());
  }

  @Test
  public void configurationFileIsExclusiveAndCannotSilentlyDiscardAnExplicitUrlEndpoint() throws Exception {
    Path file = Files.createTempFile("jdbc-endpoint-", ".properties");
    try {
      Properties config = credentials();
      config.setProperty("project_name", "endpoint_fixture");
      config.setProperty("timezone", "UTC");
      config.setProperty("odps_namespace_schema", "false");
      config.setProperty("tunnel_endpoint", second.url());
      try (OutputStream out = Files.newOutputStream(file)) { config.store(out, "synthetic fixture"); }
      download("&odps_config=" + file, new Properties(), false, false, ExecuteMode.OFFLINE);
      assertTrue(first.requests.isEmpty());
      assertFalse(second.requests.isEmpty());
      config.remove("tunnel_endpoint");
      try (OutputStream out = Files.newOutputStream(file)) { config.store(out, "synthetic fixture"); }
      SQLException failure = assertThrows(SQLException.class,
          () -> new OdpsDriver().connect(url("&odps_config=" + file), credentials()));
      assertTrue(failure.getMessage().contains("odps_config"));
      Properties explicitProperty = credentials();
      explicitProperty.setProperty("tunnel_endpoint", first.url());
      String noUrlEndpoint = url("&odps_config=" + file).replace("&tunnelEndpoint=" + first.url(), "");
      assertThrows(SQLException.class, () -> new OdpsDriver().connect(noUrlEndpoint, explicitProperty));
      assertThrows(SQLException.class, () -> new OdpsDriver().connect(
          url("&odps_config=" + file).replace("&tunnelEndpoint=", "&tunnel_endpoint="), credentials()));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  private void download(String options, Properties properties, boolean prepared, boolean scroll,
                        ExecuteMode mode) throws Exception {
    try (OdpsConnection connection = connection(options, properties);
         OdpsStatement statement = prepared
             ? connection.prepareStatement("select 42", scroll ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
             : connection.createStatement(scroll ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      Instance instance = mock(Instance.class);
      when(instance.getProject()).thenReturn("endpoint_fixture");
      when(instance.getId()).thenReturn("fixture-instance");
      when(instance.isSelect(anyString())).thenReturn(true);
      QueryInfo query = new QueryInfo("select 42", null, mode);
      query.setInstance(instance, mode, null, null);
      query.setSelect(true);
      set(statement.sqlExecutor, "queryInfo", query);
      if (mode == ExecuteMode.INTERACTIVE) { set(statement.sqlExecutor, "attachSuccess", true); }
      statement.executeInstance = instance;
      statement.inputProperties = new Properties();
      if (!options.contains("async=true")) { statement.setResultSetInternal(); }
      try (ResultSet rows = statement.getResultSet()) {
        assertTrue(rows.next());
        assertEquals(42L, rows.getLong(1));
        assertFalse(rows.next());
        if (scroll) { assertTrue(rows.first()); assertEquals(42L, rows.getLong(1)); }
      }
      assertEquals(connection.getTunnelEndpoint(), connection.getOdps().getTunnelEndpoint());
    }
  }

  private OdpsConnection connection(String options, Properties extra) throws SQLException {
    Properties p = credentials();
    p.putAll(extra);
    return new OdpsConnection(url(options), p) {
      @Override
      public MaxQAConnInfo checkIfEnableMaxQA(String quota) {
        return MaxQAConnInfo.builder().quotaName("fixture").connInfo("fixture-connection").regionId("fixture-region").build();
      }
    };
  }

  private String url(String options) {
    return "jdbc:odps:http://127.0.0.1:" + router.getAddress().getPort()
        + "?project=endpoint_fixture&timezone=UTC&odpsNamespaceSchema=false&tunnelEndpoint=" + first.url() + options;
  }

  private static Properties credentials() {
    Properties p = new Properties();
    p.setProperty("access_id", "fixture-id"); p.setProperty("access_key", "fixture-key");
    return p;
  }

  private static void set(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name); field.setAccessible(true); field.set(target, value);
  }

  private final class Endpoint {
    final HttpServer server;
    final CopyOnWriteArrayList<String> requests = new CopyOnWriteArrayList<>();
    volatile boolean fail;
    volatile boolean blobFallback;
    final AtomicInteger corruptReads = new AtomicInteger();
    Endpoint() throws IOException {
      this(false);
    }
    Endpoint(boolean tls) throws IOException {
      if (tls) {
        HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        https.setHttpsConfigurator(new HttpsConfigurator(serverTls));
        server = https;
      } else {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      }
      server.createContext("/", this::handle); server.start();
    }
    String url() { return (server instanceof HttpsServer ? "https" : "http") + "://127.0.0.1:" + server.getAddress().getPort(); }
    void handle(HttpExchange exchange) throws IOException {
      String path = exchange.getRequestURI().getPath();
      String query = String.valueOf(exchange.getRequestURI().getQuery());
      String method = exchange.getRequestMethod();
      requests.add(method + " " + path + "?" + query);
      try (java.io.InputStream in = exchange.getRequestBody()) { byte[] buffer = new byte[4096]; while (in.read(buffer) != -1) { } }
      if (fail) {
        respond(exchange, 400, "<Error><Code>InvalidParameter</Code><Message>configured endpoint failure</Message></Error>".getBytes(StandardCharsets.UTF_8));
      } else if (blobFallback && path.contains("/instances/") && method.equals("POST")) {
        respond(exchange, 400, "<Error><Code>InvalidParameter</Code><Message>download instance with blobs</Message></Error>".getBytes(StandardCharsets.UTF_8));
      } else if (path.endsWith("/api/storage/v2")) {
        respond(exchange, 400, "{\"Code\":\"NotImplemented\",\"Message\":\"stop after endpoint routing verification\"}".getBytes(StandardCharsets.UTF_8));
      } else if (path.endsWith("/upserts")) {
        String status = query.contains("upsertid=") && method.equals("POST") ? "committed" : "normal";
        String body = "{\"id\":\"upsert-fixture\",\"schema\":" + SCHEMA + ",\"hash_key\":[\"n\"],\"hasher\":\"default\",\"status\":\"" + status
            + "\",\"slots\":[{\"slot_id\":\"0\",\"buckets\":[0],\"worker_addr\":\"127.0.0.1:" + router.getAddress().getPort() + "\"}]}";
        respond(exchange, 200, body.getBytes(StandardCharsets.UTF_8));
      } else if (path.contains("/tables/")) {
        String body = "{\"UploadID\":\"upload-fixture\",\"Status\":\"NORMAL\",\"Schema\":" + SCHEMA + ",\"UploadedBlockList\":[{\"BlockID\":0}]}";
        respond(exchange, 200, body.getBytes(StandardCharsets.UTF_8));
      } else if (method.equals("GET") && path.contains("/instances/")) {
        exchange.getResponseHeaders().set("odps-tunnel-record-count", "1");
        byte[] payload = query.contains("cached") ? recordWithSchema : record;
        respond(exchange, 200, corruptReads.getAndDecrement() > 0 ? new byte[] {1} : payload);
      } else {
        String body = "{\"DownloadID\":\"download-fixture\",\"Status\":\"NORMAL\",\"RecordCount\":1,\"Schema\":" + SCHEMA + "}";
        respond(exchange, 200, body.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
  }
}
