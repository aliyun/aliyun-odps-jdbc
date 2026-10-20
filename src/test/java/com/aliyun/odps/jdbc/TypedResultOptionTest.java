package com.aliyun.odps.jdbc;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.jdbc.utils.ConnectionResource;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.QueryInfo;
import com.aliyun.odps.sqa.SQLExecutor;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/** Actual JDBC configuration and SDK CSV decoding, without cloud access or credentials. */
public class TypedResultOptionTest {
  private HttpServer server;
  private String url;
  private AtomicInteger unexpectedRequests;

  private static class TestConnection extends OdpsConnection {
    TestConnection(String url, Properties props) throws SQLException {
      super(url, props);
    }

    @Override
    public boolean checkIfEnableMaxQA(String quota) {
      return quota != null && quota.startsWith("maxqa");
    }
  }

  @Before
  public void setUp() throws Exception {
    unexpectedRequests = new AtomicInteger();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/connection/mcqa", exchange -> {
      String quota = exchange.getRequestURI().getQuery().contains("maxqa_second")
          ? "maxqa_second" : "maxqa_fixture";
      byte[] body = ("{\"quotaNickName\":\"" + quota
          + "\",\"connInfo\":\"fixture-connection\",\"regionId\":\"fixture-region\"}")
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    // SDK 0.61 emits query telemetry; keep it local without allowing result downloads.
    server.createContext("/projects/typed_fixture/logs", exchange -> {
      try (java.io.InputStream in = exchange.getRequestBody()) {
        while (in.read() != -1) { }
      }
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.createContext("/", exchange -> {
      unexpectedRequests.incrementAndGet();
      System.err.println("Unexpected fixture path: " + exchange.getRequestURI().getPath());
      byte[] body = "<Error><Code>InvalidParameter</Code><Message>unexpected request</Message></Error>"
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(400, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    url = "jdbc:odps:http://127.0.0.1:" + server.getAddress().getPort()
        + "?project=typed_fixture&interactiveMode=MaxQA&quotaName=maxqa_fixture"
        + "&timezone=UTC&odpsNamespaceSchema=false&useInstanceTunnel=false";
  }

  @After
  public void tearDown() {
    server.stop(0);
    assertEquals(0, unexpectedRequests.get());
  }

  private Properties props() {
    Properties p = new Properties();
    p.setProperty("access_id", "offline-fixture-id");
    p.setProperty("access_key", "offline-fixture-key");
    p.setProperty("project_name", "typed_fixture");
    return p;
  }

  @Test
  public void defaultAndExplicitTruePreserveTypedValues() throws Exception {
    for (String option : new String[]{"", "&enableTypedResult=true", "&enableTypedResult=TRUE"}) {
      verify(url + option, props(), true);
    }
  }

  @Test
  public void urlFalseReturnsStringsAndStringMetadata() throws Exception {
    verify(url + "&enableTypedResult=FALSE", props(), false);
  }

  @Test
  public void propertiesTakePrecedenceOverUrlInBothDirections() throws Exception {
    Properties p = props();
    p.setProperty("enable_typed_result", "FaLsE");
    verify(url + "&enableTypedResult=true", p, false);
    p.setProperty("enable_typed_result", "true");
    verify(url + "&enableTypedResult=false", p, true);
  }

  @Test
  public void configurationFileKeepsItsExclusivePrecedence() throws Exception {
    Path file = Files.createTempFile("jdbc-typed-result", ".properties");
    try {
      Properties fileProps = props();
      fileProps.setProperty("enable_typed_result", "false");
      fileProps.setProperty("interactive_mode", "MaxQA");
      fileProps.setProperty("quota_name", "maxqa_fixture");
      fileProps.setProperty("use_instance_tunnel", "false");
      fileProps.setProperty("odps_namespace_schema", "false");
      fileProps.setProperty("timezone", "UTC");
      try (OutputStream out = Files.newOutputStream(file)) {
        fileProps.store(out, "Synthetic offline test configuration");
      }
      Properties p = props();
      p.setProperty("enable_typed_result", "true");
      verify(url + "&enableTypedResult=true&odps_config=" + file, p, false);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidBooleanValuesBeforeOpeningAConnection() {
    for (String invalid : new String[]{"yes", "0", ""}) {
      assertThrows(IllegalArgumentException.class,
          () -> new ConnectionResource(url + "&enableTypedResult=" + invalid, props()));
      Properties p = props();
      p.setProperty("enable_typed_result", invalid);
      assertThrows(IllegalArgumentException.class, () -> new ConnectionResource(url, p));
    }
  }

  @Test
  public void driverPropertyInfoExposesTheBooleanChoice() throws Exception {
    for (boolean enabled : new boolean[]{false, true}) {
      DriverPropertyInfo[] info = new OdpsDriver().getPropertyInfo(
          url + "&enableTypedResult=" + enabled, props());
      DriverPropertyInfo option = null;
      for (DriverPropertyInfo candidate : info) {
        if ("enable_typed_result".equals(candidate.name)) {
          option = candidate;
        }
      }
      assertNotNull(option);
      assertEquals(Boolean.toString(enabled), option.value);
      assertArrayEquals(new String[]{"true", "false"}, option.choices);
      assertFalse(option.required);
    }
  }

  @Test
  public void executorRebuildKeepsTheConfiguredResultMode() throws Exception {
    for (boolean enabled : new boolean[]{false, true}) {
      try (OdpsConnection conn = new TestConnection(url + "&enableTypedResult=" + enabled, props());
           OdpsStatement stmt = conn.createStatement()) {
        Properties settings = new Properties();
        settings.setProperty("jdbc.tunnel.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        stmt.processSetClauseExtra(settings);
        assertNotSame(conn.getExecutor(), stmt.sqlExecutor);
        verifyResults(conn, stmt, enabled);
      }
    }
  }

  @Test
  public void dynamicQuotaSwitchPreservesUntypedResults() throws Exception {
    try (OdpsConnection conn = new TestConnection(url + "&enableTypedResult=false", props());
         OdpsStatement stmt = conn.createStatement()) {
      Properties settings = new Properties();
      settings.setProperty("odps.task.wlm.quota", "maxqa_second");
      stmt.processSetClauseExtra(settings);
      assertNotSame(conn.getExecutor(), stmt.sqlExecutor);
      verifyResults(conn, stmt, false);
    }
  }

  @Test
  public void disablingTypedResultsDoesNotDisableTunnel() throws Exception {
    String tunnelUrl = url.replace("useInstanceTunnel=false", "useInstanceTunnel=true");
    try (OdpsConnection conn = new TestConnection(tunnelUrl + "&enableTypedResult=false", props())) {
      assertTrue(conn.getExecutorBuilder().isUseInstanceTunnel());
      assertFalse(conn.getExecutorBuilder().isEnableTypedResult());
    }
  }

  private void verify(String connectionUrl, Properties p, boolean enabled) throws Exception {
    try (OdpsConnection conn = new TestConnection(connectionUrl, p);
         OdpsStatement stmt = conn.createStatement()) {
      verifyResults(conn, stmt, enabled);
    }
  }

  private void verifyResults(OdpsConnection conn, OdpsStatement stmt, boolean enabled)
      throws Exception {
    stmt.inputProperties = new Properties();
    SQLExecutor executor = stmt.sqlExecutor;
    QueryInfo query = new QueryInfo("select fixture", null, ExecuteMode.INTERACTIVE_V2);
    query.setInstance(cachedInstance(conn.getOdps(), executor.getTaskName()),
        ExecuteMode.INTERACTIVE_V2, null, null);
    query.setSelect(true);
    set(executor, "queryInfo", query);
    stmt.odpsResultSet = executor.getResultSet();
    try (ResultSet jdbc = stmt.getResultSet()) {
      assertTrue(jdbc.next());
      if (enabled) {
        assertEquals(Double.valueOf(25.5), jdbc.getObject(1));
        assertEquals(Long.valueOf(1), jdbc.getObject(2));
        assertEquals(Long.valueOf(9007199254740993L), jdbc.getObject(3));
        assertEquals("hello", jdbc.getObject(4));
        assertNull(jdbc.getObject(5));
        assertTrue(jdbc.wasNull());
        assertEquals("DOUBLE", jdbc.getMetaData().getColumnTypeName(1));
        assertEquals("BIGINT", jdbc.getMetaData().getColumnTypeName(2));
      } else {
        String[] expected = {"25.5", "1", "9007199254740993", "hello", "\\N"};
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], jdbc.getObject(i + 1));
          assertEquals("STRING", jdbc.getMetaData().getColumnTypeName(i + 1));
          assertEquals(String.class.getName(), jdbc.getMetaData().getColumnClassName(i + 1));
        }
      }
      assertFalse(jdbc.next());
    }
  }

  private Instance cachedInstance(Odps odps, String taskName) throws Exception {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("double_col", OdpsType.DOUBLE));
    schema.addColumn(new Column("bigint_col", OdpsType.BIGINT));
    schema.addColumn(new Column("large_col", OdpsType.BIGINT));
    schema.addColumn(new Column("text_col", OdpsType.STRING));
    schema.addColumn(new Column("null_col", OdpsType.STRING));
    Instance.Result response = new Instance.Result();
    set(response, "format", "csv");
    set(response, "text", "double_col,bigint_col,large_col,text_col,null_col\n"
        + "25.5,1,9007199254740993,hello,\\N\n");
    Constructor<Instance.ResultDescriptor> descriptorCtor =
        Instance.ResultDescriptor.class.getDeclaredConstructor();
    descriptorCtor.setAccessible(true);
    Instance.ResultDescriptor descriptor = descriptorCtor.newInstance();
    descriptor.setIsSelect(true);
    descriptor.setSchema(schema);
    descriptor.setSelectResultStatus(Instance.ResultDescriptor.SelectResultStatus.FULL);
    Instance.InstanceResultModel.TaskResult task = new Instance.InstanceResultModel.TaskResult();
    set(task, "name", taskName);
    set(task, "status", "success");
    set(task, "result", response);
    set(task, "resultDescriptor", descriptor);
    Class<?> statusModel = Class.forName("com.aliyun.odps.Instance$TaskStatusModel");
    Constructor<?> modelCtor = statusModel.getDeclaredConstructor();
    modelCtor.setAccessible(true);
    Object model = modelCtor.newInstance();
    set(model, "name", "offline-fixture");
    Map<String, Instance.InstanceResultModel.TaskResult> tasks = new HashMap<>();
    tasks.put(taskName, task);
    Constructor<Instance> instanceCtor = Instance.class.getDeclaredConstructor(
        String.class, statusModel, Map.class, Odps.class);
    instanceCtor.setAccessible(true);
    return instanceCtor.newInstance("typed_fixture", model, tasks, odps);
  }

  private static void set(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
