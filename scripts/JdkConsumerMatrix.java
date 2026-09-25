/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Consumer-perspective compatibility probe for the packaged MaxCompute JDBC driver.
 *
 * <p>It uses the driver the way a consumer does: the packaged jar is the only library on the
 * classpath, the entry point is {@code java.sql.DriverManager}, and every observable is printed
 * as a {@code key=value} line. Running this probe under several JVMs against the same jar and
 * diffing the key/value sets is what turns "it works on my JDK" into a compatibility matrix.
 *
 * <p>Cells:
 * <ul>
 *   <li>{@code classloading} - service-file registration, driver metadata, and initialization of
 *       the driver plus the relocated third-party code that only exists in the packaged jar.</li>
 *   <li>{@code core-contract} - connection, DatabaseMetaData and statement surface. Runs against
 *       a closed loopback port with {@code timezone}, {@code odpsNamespaceSchema=false} and
 *       {@code useInstanceTunnel=false}, the three switches that keep the driver constructor from
 *       reaching out, so no credentials and no service are needed.</li>
 *   <li>{@code resource-close} - close/idempotency/use-after-close behaviour, plus the non-daemon
 *       thread accounting that decides whether a consumer JVM can terminate.</li>
 *   <li>{@code live-sql} - read-only SQL against the real service when the four environment
 *       variables are present. Without credentials the cell is reported as an explicit
 *       {@code SKIPPED}, never as a pass.</li>
 * </ul>
 *
 * <p>On the happy path the program does not call {@code System.exit}: if something leaves a
 * non-daemon thread behind, the JVM hangs and the runner reports the cell as a timeout, which is
 * exactly the symptom a consumer sees.
 */
public final class JdkConsumerMatrix {

  private static final String DRIVER_CLASS = "com.aliyun.odps.jdbc.OdpsDriver";
  private static final String ARROW_ALLOCATOR =
      "com.aliyun.odps.jdbc.shaded.org.apache.arrow.memory.RootAllocator";
  private static int failures = 0;
  private static int checks = 0;

  private JdkConsumerMatrix() {}

  /** An observable that may legitimately throw; the exception type is the value. */
  private interface Probe {
    Object call() throws Exception;
  }

  // ------------------------------------------------------------------ reporting

  /** Values are diffed across JVMs by the runner, so unstable data is normalized right here. */
  private static void emit(String key, Object value) {
    System.out.println(key + "=" + String.valueOf(value).replace('\n', ' ').replace('\r', ' '));
  }

  private static void check(String key, boolean condition, Object observed) {
    checks++;
    if (!condition) {
      failures++;
      System.out.println("FAILED " + key + " expected=true observed=" + observed);
    }
    emit(key, condition ? "true" : "false/" + observed);
  }

  private static String value(Probe probe) {
    try {
      Object result = probe.call();
      return result == null ? "null" : String.valueOf(result);
    } catch (Throwable t) {
      String detail = t.getClass().getName();
      if (t instanceof ExceptionInInitializerError && ((ExceptionInInitializerError) t)
          .getException() != null) {
        Throwable cause = ((ExceptionInInitializerError) t).getException();
        detail += "<-" + cause.getClass().getName();
        if (cause.getMessage() != null) {
          detail += ":" + cause.getMessage().replace('/', '.');
        }
      } else if (t instanceof NoClassDefFoundError || t instanceof ClassNotFoundException) {
        // The message of a linkage error is a class name, which is what a consumer needs.
        detail += t.getMessage() == null ? "" : ":" + t.getMessage().replace('/', '.');
      }
      return detail;
    }
  }

  /** Non-daemon threads alive right now, as identity sets, for leak attribution per scenario. */
  private static Set<Thread> threadSnapshot() {
    return new HashSet<>(Thread.getAllStackTraces().keySet());
  }

  /**
   * Non-daemon threads that were not in {@code baseline} and are still alive, with pool names
   * digit-normalized so the value can be diffed across JVMs.
   */
  private static String newNonDaemonThreadsSince(Set<Thread> baseline) {
    TreeSet<String> names = new TreeSet<>();
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      Thread thread = entry.getKey();
      if (thread == Thread.currentThread() || !thread.isAlive() || thread.isDaemon()
          || baseline.contains(thread)) {
        continue;
      }
      names.add(thread.getName().replaceAll("[0-9]+", "N"));
    }
    return names.toString();
  }

  private static String threadPatterns() {
    return newNonDaemonThreadsSince(Collections.<Thread>emptySet());
  }

  /**
   * Waits for the threads created since {@code baseline} to go away. Idle worker threads of a
   * bounded pool are not a leak, they expire: the REST client in the packaged jar builds
   * {@code new ThreadPoolExecutor(0, 3, 10L, SECONDS, ...)}, so "still there 6 s later" was my own
   * measurement being too impatient. Anything still alive after 20 s is a thread a consumer has to
   * live with. Because the comparison is against a per-scenario baseline, a thread created before
   * the first cell is not reported here; the runner's cell timeout is what catches that.
   */
  private static String awaitSettledThreads(Set<Thread> baseline, long millis) {
    long deadline = System.currentTimeMillis() + millis;
    String observed = newNonDaemonThreadsSince(baseline);
    while (!"[]".equals(observed) && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      observed = newNonDaemonThreadsSince(baseline);
    }
    return observed;
  }

  /**
   * Reports an observable that departs from the JDBC specification without failing the cell. The
   * matrix is about JVM divergence; a defect that behaves identically on every JVM belongs to its
   * own issue. If it ever becomes JVM-specific, the parity diff shows it.
   */
  private static void specNote(String key, String observed, String expected, String rule) {
    if (!expected.equals(observed)) {
      System.out.println("FINDING " + key + "=" + observed + " (" + rule + ", expected "
          + expected + ")");
    }
  }

  private static String bucket(long millis) {
    if (millis < 1000) {
      return "lt-1s";
    }
    return millis < 5000 ? "1-5s" : "ge-5s";
  }

  // ------------------------------------------------------------------ helpers

  /** A port in the ephemeral range that nothing listens on once the socket is released. */
  private static int closedLoopbackPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static Properties driverProperties() {
    Properties properties = new Properties();
    properties.setProperty("access_id", "matrix-placeholder");
    properties.setProperty("access_key", "matrix-placeholder");
    properties.setProperty("project_name", "matrix-placeholder");
    properties.setProperty("log_level", "OFF");
    return properties;
  }

  /** Real driver object, no service needed: see the three switches in the class comment. */
  private static Connection offlineConnection(int port) throws Exception {
    String url = "jdbc:odps:http://127.0.0.1:" + port + "/api"
        + "?timezone=Asia/Shanghai&odpsNamespaceSchema=false&useInstanceTunnel=false";
    return DriverManager.getConnection(url, driverProperties());
  }

  private static boolean hasLiveEnvironment() {
    String[] names = {"ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "MAXCOMPUTE_PROJECT", "MAXCOMPUTE_ENDPOINT"};
    for (String name : names) {
      String value = System.getenv(name);
      if (value == null || value.trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  // ------------------------------------------------------------------ cells

  private static void runtimeCell() {
    emit("runtime_java_version", System.getProperty("java.version"));
    emit("runtime_vm_name", System.getProperty("java.vm.name"));
    emit("runtime_spec_version", System.getProperty("java.specification.version"));
    emit("runtime_cpu_cores_bucket",
        Runtime.getRuntime().availableProcessors() < 8 ? "lt8" : "ge8");
  }

  private static void classloadingCell() {
    List<String> discovered = new ArrayList<>();
    boolean registeredByServiceFile = false;
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      String name = drivers.nextElement().getClass().getName();
      discovered.add(name);
      if (DRIVER_CLASS.equals(name)) {
        registeredByServiceFile = true;
      }
    }
    Collections.sort(discovered);
    emit("drivers_visible_before_forname", discovered.toString());
    check("driver_registered_by_service_file", registeredByServiceFile, discovered);

    final String url = "jdbc:odps:http://127.0.0.1:1/api";
    emit("driver_class_load", value(new Probe() {
      public Object call() throws Exception { return Class.forName(DRIVER_CLASS).getName(); }
    }));
    // A consumer reaches the driver through DriverManager, which is also how the service file
    // registration is proven. getDriver() avoids reflective instantiation entirely.
    emit("driver_from_drivermanager", value(new Probe() {
      public Object call() throws Exception {
        Driver driver = DriverManager.getDriver(url);
        return driver.getClass().getName() + " " + driver.getMajorVersion()
            + "." + driver.getMinorVersion() + " jdbcCompliant=" + driver.jdbcCompliant()
            + " accepts=" + driver.acceptsURL(url)
            + " acceptsOther=" + driver.acceptsURL("jdbc:other:x")
            + " propertyInfo=" + driver.getPropertyInfo(url, new Properties()).length;
      }
    }));

    // Driver-facing classes, and the relocated code that only exists inside the packaged jar.
    String[] required = {
        DRIVER_CLASS,
        "com.aliyun.odps.jdbc.OdpsConnection",
        "com.aliyun.odps.jdbc.OdpsStatement",
        "com.aliyun.odps.jdbc.OdpsPreparedStatement",
        "com.aliyun.odps.jdbc.OdpsResultSet",
        "com.aliyun.odps.jdbc.OdpsResultSetMetaData",
        "com.aliyun.odps.jdbc.OdpsDatabaseMetaData",
        "com.aliyun.odps.jdbc.utils.ConnectionResource",
        "com.aliyun.odps.jdbc.utils.InstanceDataIterator",
        "com.aliyun.odps.jdbc.shaded.io.netty.buffer.ByteBufUtil",
        "com.aliyun.odps.jdbc.shaded.com.fasterxml.jackson.databind.ObjectMapper",
        "com.aliyun.odps.jdbc.shaded.com.google.protobuf.CodedInputStream",
    };
    List<String> unloadable = new ArrayList<>();
    for (final String className : required) {
      String linkOutcome = value(new Probe() {
        public Object call() throws Exception {
          Class.forName(className, false, JdkConsumerMatrix.class.getClassLoader());
          return "OK";
        }
      });
      if (!"OK".equals(linkOutcome)) {
        unloadable.add(className + "=" + linkOutcome);
      }
    }
    emit("required_classes_link_failures", unloadable.toString());
    check("required_classes_all_link", unloadable.isEmpty(), unloadable);

    // Static initializers: relocated Arrow logs through org.slf4j, which the pom keeps in
    // provided scope on purpose, so a bare consumer classpath may not have it.
    emit("init_sdk_odps", value(new Probe() {
      public Object call() throws Exception { Class.forName("com.aliyun.odps.Odps"); return "OK"; }
    }));
    final String arrowOutcome = value(new Probe() {
      public Object call() throws Exception { Class.forName(ARROW_ALLOCATOR); return "OK"; }
    });
    emit("init_relocated_arrow_allocator", arrowOutcome);
    final String slf4jOutcome = value(new Probe() {
      public Object call() throws Exception {
        Class.forName("org.slf4j.LoggerFactory");
        return "OK";
      }
    });
    emit("slf4j_on_consumer_classpath", slf4jOutcome);
    if (arrowOutcome.indexOf("org.slf4j") >= 0 && slf4jOutcome.indexOf("org.slf4j") >= 0) {
      System.out.println("FINDING relocated Arrow classes need org.slf4j, which the build keeps "
          + "in provided scope: a consumer that runs the packaged jar alone hits this the first "
          + "time an Arrow code path is initialized. Same on every JVM, so it is reported as a "
          + "finding rather than a JVM-specific failure.");
    }
  }

  private static void coreContractCell() throws Exception {
    long start = System.currentTimeMillis();
    int port = closedLoopbackPort();
    final Connection connection;
    try {
      connection = offlineConnection(port);
    } catch (Exception e) {
      check("offline_connect", false, e.getClass().getName());
      return;
    }
    check("offline_connect", true, "connected");
    emit("offline_connect_latency_bucket", bucket(System.currentTimeMillis() - start));

    final DatabaseMetaData meta = connection.getMetaData();
    emit("meta_product", meta.getDatabaseProductName() + "/" + meta.getDatabaseProductVersion());
    emit("meta_driver", meta.getDriverName() + "/" + meta.getDriverVersion());
    emit("meta_jdbc_version", meta.getJDBCMajorVersion() + "." + meta.getJDBCMinorVersion());
    emit("conn_autocommit", connection.getAutoCommit());
    emit("conn_tx_isolation", connection.getTransactionIsolation());
    emit("conn_readonly", value(new Probe() {
      public Object call() throws Exception { return connection.isReadOnly(); }
    }));
    emit("meta_supports_batch", value(new Probe() {
      public Object call() throws Exception { return meta.supportsBatchUpdates(); }
    }));
    emit("meta_supports_transactions", value(new Probe() {
      public Object call() throws Exception { return meta.supportsTransactions(); }
    }));
    emit("meta_supports_generated_keys", value(new Probe() {
      public Object call() throws Exception { return meta.supportsGetGeneratedKeys(); }
    }));
    emit("meta_supports_stored_procedures", value(new Probe() {
      public Object call() throws Exception { return meta.supportsStoredProcedures(); }
    }));
    emit("meta_sql_keywords_empty", value(new Probe() {
      public Object call() throws Exception { return Boolean.valueOf(meta.getSQLKeywords().isEmpty()); }
    }));
    emit("meta_resultset_concurrency_only_read_forward", value(new Probe() {
      public Object call() throws Exception {
        return meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
      }
    }));
    emit("conn_isvalid", value(new Probe() {
      public Object call() throws Exception { return connection.isValid(2); }
    }));
    emit("conn_warnings", value(new Probe() {
      public Object call() throws Exception { return connection.getWarnings(); }
    }));

    // Reaching a dead port must fail. What the matrix compares is *how* it fails per JVM.
    emit("exec_on_unreachable_endpoint", value(new Probe() {
      public Object call() throws Exception {
        Statement statement = connection.createStatement();
        try {
          statement.executeQuery("select 1");
        } finally {
          statement.close();
        }
        return "no-exception";
      }
    }));
    emit("prepare_statement_created", value(new Probe() {
      public Object call() throws Exception { connection.prepareStatement("select ?"); return "created"; }
    }));
    // The probe closes what it opens, so the next cell's thread accounting starts clean.
    connection.close();
  }

  private static void resourceCloseCell() throws Exception {
    int port = closedLoopbackPort();
    // Baseline before connecting: threads created by the connection itself belong to this cell.
    final Set<Thread> opened = threadSnapshot();
    final Connection connection = offlineConnection(port);
    final Statement statement = connection.createStatement();
    final PreparedStatement prepared = connection.prepareStatement("select 1");
    emit("threads_with_open_resources", threadPatterns());

    statement.close();
    emit("statement_is_closed", statement.isClosed());
    emit("statement_double_close", value(new Probe() {
      public Object call() throws Exception { statement.close(); return "no-exception"; }
    }));
    final String statementUseAfterClose = value(new Probe() {
      public Object call() throws Exception { statement.executeQuery("select 1"); return "no-exception"; }
    });
    emit("statement_use_after_close", statementUseAfterClose);
    specNote("statement_use_after_close", statementUseAfterClose, "java.sql.SQLException",
        "JDBC requires SQLException on a closed Statement");
    prepared.close();
    emit("prepared_is_closed", prepared.isClosed());
    emit("prepared_double_close", value(new Probe() {
      public Object call() throws Exception { prepared.close(); return "no-exception"; }
    }));
    final String preparedUseAfterClose = value(new Probe() {
      public Object call() throws Exception { prepared.executeQuery(); return "no-exception"; }
    });
    emit("prepared_use_after_close", preparedUseAfterClose);
    specNote("prepared_use_after_close", preparedUseAfterClose, "java.sql.SQLException",
        "JDBC requires SQLException on a closed PreparedStatement");

    connection.close();
    emit("connection_is_closed", connection.isClosed());
    emit("connection_double_close", value(new Probe() {
      public Object call() throws Exception { connection.close(); return "no-exception"; }
    }));
    final String connectionUseAfterClose = value(new Probe() {
      public Object call() throws Exception { connection.createStatement(); return "no-exception"; }
    });
    emit("connection_use_after_close", connectionUseAfterClose);
    specNote("connection_use_after_close", connectionUseAfterClose, "java.sql.SQLException",
        "JDBC requires SQLException on a closed Connection");
    emit("threads_after_connection_close", threadPatterns());
    final String leftoverAfterClose = awaitSettledThreads(opened, 20000);
    emit("threads_after_close_settled", leftoverAfterClose);
    check("no_non_daemon_thread_left_behind", "[]".equals(leftoverAfterClose), leftoverAfterClose);
  }

  private static void liveCell() {
    if (!hasLiveEnvironment()) {
      emit("live_sql", "SKIPPED:no-credentials-in-environment");
      return;
    }
    final Properties properties = liveProperties();
    final String url = "jdbc:odps:" + System.getenv("MAXCOMPUTE_ENDPOINT");
    check("live_connect", "valid".equals(liveOutcome(properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception { return c.isValid(5) ? "valid" : "invalid"; }
    })), "connect");
    check("live_scalar_query", "OK".equals(liveOutcome(properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        Statement s = c.createStatement();
        try {
          ResultSet rs = s.executeQuery("select cast(42 as bigint) as n");
          try {
            if (!rs.next()) {
              return "no-row";
            }
            return rs.getLong(1) == 42 ? "OK" : "wrong-value";
          } finally {
            rs.close();
          }
        } finally {
          s.close();
        }
      }
    })), "scalar");
    check("live_bound_query", "OK".equals(liveOutcome(properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        PreparedStatement ps = c.prepareStatement("select ? as s");
        try {
          ps.setString(1, "matrix");
          ResultSet rs = ps.executeQuery();
          try {
            if (!rs.next()) {
              return "no-row";
            }
            return "matrix".equals(rs.getString(1)) ? "OK" : "wrong-value";
          } finally {
            rs.close();
          }
        } finally {
          ps.close();
        }
      }
    })), "bound");
    check("live_metadata_columns", "called".equals(liveOutcome(properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        DatabaseMetaData md = c.getMetaData();
        ResultSet rs = md.getColumns(null, null, "%", null);
        try {
          int rows = 0;
          while (rs.next() && rows < 50) {
            rows++;
          }
          return "called";
        } finally {
          rs.close();
        }
      }
    })), "metadata");
    // Each scenario gets its own thread baseline taken before the connection is opened, so a
    // pool left behind by an earlier scenario cannot be blamed on a later one.
    threadCell("live_statement_reuse_threads", properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        Statement st = c.createStatement();
        try {
          for (int i = 0; i < 2; i++) {
            ResultSet rs = st.executeQuery("select cast(" + i + " as bigint) as n");
            try {
              while (rs.next()) {
                rs.getLong(1);
              }
            } finally {
              rs.close();
            }
          }
        } finally {
          st.close();
        }
        return "reused-twice";
      }
    });
    threadCell("live_partial_read_threads", properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        Statement st = c.createStatement();
        try {
          ResultSet rs = st.executeQuery("select cast(1 as bigint) as n union all "
              + "select cast(2 as bigint) as n union all select cast(3 as bigint) as n");
          try {
            rs.next();
          } finally {
            rs.close();
          }
        } finally {
          st.close();
        }
        return "one-row-then-close";
      }
    });
    threadCell("live_scrollable_threads", properties, url, new OnConnection() {
      public Object call(Connection c) throws Exception {
        Statement st;
        try {
          st = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException refused) {
          return "scrollable-not-supported:" + refused.getSQLState();
        }
        try {
          ResultSet rs = st.executeQuery("select cast(1 as bigint) as n union all "
              + "select cast(2 as bigint) as n");
          try {
            while (rs.next()) {
              rs.getLong(1);
            }
            rs.beforeFirst();
          } finally {
            rs.close();
          }
        } finally {
          st.close();
        }
        return "scrollable-read-and-rewound";
      }
    });
    emit("live_arrow_allocator_after_queries", value(new Probe() {
      public Object call() throws Exception { Class.forName(ARROW_ALLOCATOR); return "OK"; }
    }));
    emit("threads_after_live_close", threadPatterns());
  }

  private static Properties liveProperties() {
    Properties properties = new Properties();
    properties.setProperty("access_id", System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"));
    properties.setProperty("access_key", System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    properties.setProperty("project_name", System.getenv("MAXCOMPUTE_PROJECT"));
    properties.setProperty("log_level", "OFF");
    String token = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");
    if (token != null && !token.trim().isEmpty()) {
      properties.setProperty("sts_token", token);
    }
    return properties;
  }

  /** Runs a body on a live connection, then reports threads created by that scenario only. */
  private static void threadCell(String key, Properties properties, String url,
      OnConnection body) {
    Set<Thread> baseline = threadSnapshot();
    String outcome = liveOutcome(properties, url, body);
    String leftover = awaitSettledThreads(baseline, 20000);
    emit(key + "_outcome", outcome);
    emit(key, leftover);
    check(key + "_settled", "[]".equals(leftover), leftover);
  }

  /** Opens a real connection, runs the body, closes it; failures reduce to the throwable type. */
  private static String liveOutcome(Properties properties, String url, OnConnection body) {
    Connection connection = null;
    try {
      Class.forName(DRIVER_CLASS);
      connection = DriverManager.getConnection(url, properties);
      Object result = body.call(connection);
      return result == null ? "null" : String.valueOf(result);
    } catch (Throwable t) {
      // No message here on purpose: live failures embed endpoints and instance ids.
      return t.getClass().getName();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception ignored) {
          // Closing must not mask the outcome being reported.
        }
      }
    }
  }

  private interface OnConnection {
    Object call(Connection connection) throws Exception;
  }

  private static void summary() {
    emit("checks_total", checks);
    emit("checks_failed", failures);
    System.out.println(failures == 0
        ? "MATRIX_CELL_RESULT=PASS" : "MATRIX_CELL_RESULT=FAIL(" + failures + ")");
  }

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    emit("cell_runtime", "begun");
    runtimeCell();
    emit("cell_classloading", "begun");
    classloadingCell();
    emit("cell_core_contract", "begun");
    coreContractCell();
    emit("cell_resource_close", "begun");
    resourceCloseCell();
    emit("cell_live_sql", "begun");
    liveCell();
    emit("elapsed_ms_bucket", bucket(System.currentTimeMillis() - start));
    summary();
  }
}
