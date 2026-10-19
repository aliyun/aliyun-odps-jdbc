import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Packaged-driver pressure probe; each worker owns its connection and JDBC resources. */
public final class JdbcSqlPerformance {
  private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();
  private static final AtomicLong PEAK_HEAP = new AtomicLong();
  private static volatile boolean sampling = true;

  private static void check(boolean ok, String message) {
    if (!ok) throw new AssertionError(message);
  }

  private static String repeat(String part, int chars) {
    StringBuilder b = new StringBuilder(chars + part.length());
    while (b.length() < chars) b.append(part);
    b.setLength(chars);
    return b.toString();
  }

  private static long gc(boolean time) {
    long result = 0;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long value = time ? bean.getCollectionTime() : bean.getCollectionCount();
      if (value >= 0) result += value;
    }
    return result;
  }

  private static Connection connect(String endpoint, boolean parserControl) throws Exception {
    Properties p = new Properties();
    p.setProperty("access_id", "test-ak"); p.setProperty("access_key", "test-sk");
    p.setProperty("project_name", "test_project"); p.setProperty("tunnelEndpoint", endpoint);
    p.setProperty("enableLimit", "false"); p.setProperty("log_level", "OFF");
    if (parserControl) p.setProperty("skipCheckIfSelect", "false");
    Connection c = DriverManager.getConnection("jdbc:odps:" + endpoint, p);
    java.lang.reflect.Field flag = c.getClass().getDeclaredField("skipCheckIfSelect");
    flag.setAccessible(true);
    boolean skips = flag.getBoolean(c);
    check(skips != parserControl, "unexpected skipCheckIfSelect default/control");
    return c;
  }

  private static final class Workload {
    final String name, sql, value;
    final int parameters;
    Workload(String name, int chars) {
      this.name = name;
      if (name.equals("statement-comment")) {
        sql = "set odps.sql.type.system.odps2=true; /*" + repeat("x", Math.max(1, chars - 64)) + "*/ select 73 as v";
        value = null; parameters = 0;
      } else if (name.equals("statement-tokens") || name.equals("prepared-tokens")) {
        StringBuilder b = new StringBuilder(name.startsWith("prepared") ? "select ? as v" : "select 73 as v");
        b.append(" where 1 in (1");
        while (b.length() < chars) b.append(",1");
        sql = b.append(")").toString(); value = null;
        parameters = name.startsWith("prepared") ? 1 : 0;
      } else if (name.equals("prepared-many") || name.equals("batch-invalid")) {
        int count = Math.max(2, chars / 2);
        StringBuilder b = new StringBuilder(name.equals("batch-invalid") ? "insert into perf_absent values (?" : "select ? as v where 1 in (?");
        for (int i = 1; i < count; i++) b.append(",?");
        sql = b.append(name.equals("batch-invalid") ? ") invalid_suffix" : ")").toString();
        value = null; parameters = name.equals("batch-invalid") ? count : count + 1;
      } else if (name.equals("literal-open") || name.equals("literal-closed")) {
        sql = "select ? as v"; parameters = 1;
        value = name.equals("literal-open") ? repeat("/*x", Math.max(1, chars - 64)) : "/*" + repeat("x", chars) + "*/";
      } else if (name.equals("parser-control")) {
        sql = "select 73 as v"; value = null; parameters = 0;
      } else throw new IllegalArgumentException("Unknown workload " + name);
    }

    void once(Connection c) throws Exception {
      if (parameters == 0) {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) { verify(r); }
      } else {
        try (PreparedStatement p = c.prepareStatement(sql)) {
          if (value != null) p.setString(1, value);
          else for (int i = 1; i <= parameters; i++) p.setInt(i, i == 1 && !name.equals("batch-invalid") ? 73 : 1);
          if (name.equals("batch-invalid")) {
            p.addBatch();
            try { p.executeBatch(); throw new AssertionError("invalid batch SQL was accepted"); }
            catch (SQLException e) { check(e.getMessage().contains("batched statement only support"), e.toString()); }
            finally { p.clearBatch(); }
          } else if (name.equals("literal-closed")) {
            try (ResultSet ignored = p.executeQuery()) { throw new AssertionError("complete comment was accepted"); }
            catch (SQLException e) { check(e.getMessage().contains("SQL injection check"), e.toString()); }
          } else try (ResultSet r = p.executeQuery()) { verify(r); }
        }
      }
    }

    void verify(ResultSet r) throws Exception {
      check(r.next(), "missing row");
      if (value == null) check(r.getInt(1) == 73, "wrong numeric result");
      else check(value.equals(r.getString(1)), "parameter value changed");
      check(!r.next(), "extra row");
    }
  }

  private static double percentile(long[] times, double percentile) {
    return times[Math.max(0, (int)Math.ceil(times.length * percentile) - 1)] / 1e6;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args[0], name = args[1];
    check("127.0.0.1".equals(new java.net.URI(endpoint).getHost()), "loopback emulator required");
    int chars = Integer.parseInt(args[2]), workers = Integer.parseInt(args[3]), iterations = Integer.parseInt(args[4]);
    check(chars > 0 && workers > 0 && iterations > 0, "positive dimensions required");
    Class.forName("com.aliyun.odps.jdbc.OdpsDriver");
    Workload workload = new Workload(name, chars);
    Thread sampler = new Thread(() -> {
      while (sampling) {
        PEAK_HEAP.accumulateAndGet(MEMORY.getHeapMemoryUsage().getUsed(), Math::max);
        try { Thread.sleep(10); } catch (InterruptedException e) { return; }
      }
    }, "jdbc-perf-heap-sampler");
    sampler.setDaemon(true); sampler.start();
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    CountDownLatch ready = new CountDownLatch(workers), start = new CountDownLatch(1);
    long[] latencies = new long[workers * iterations];
    List<Future<?>> jobs = new ArrayList<>();
    try {
      for (int w = 0; w < workers; w++) {
        final int worker = w;
        jobs.add(pool.submit(() -> {
          try (Connection c = connect(endpoint, name.equals("parser-control"))) {
            workload.once(c); // Functional warmup is excluded from latency samples.
            ready.countDown(); start.await();
            for (int i = 0; i < iterations; i++) {
              long before = System.nanoTime(); workload.once(c);
              latencies[worker * iterations + i] = System.nanoTime() - before;
            }
          } catch (Throwable e) { ready.countDown(); throw new RuntimeException(e); }
          return null;
        }));
      }
      check(ready.await(45, TimeUnit.SECONDS), "connection/warmup deadline exceeded");
      for (Future<?> job : jobs) if (job.isDone()) job.get();
      System.gc(); long heapBefore = MEMORY.getHeapMemoryUsage().getUsed();
      long gcCount = gc(false), gcMs = gc(true), before = System.nanoTime();
      long cpuBefore = ((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
      start.countDown(); pool.shutdown();
      for (Future<?> job : jobs) job.get(60, TimeUnit.SECONDS);
      check(pool.awaitTermination(5, TimeUnit.SECONDS), "worker pool did not terminate");
      double elapsedMs = (System.nanoTime() - before) / 1e6;
      long cpuNs = ((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() - cpuBefore;
      System.gc(); long heapAfter = MEMORY.getHeapMemoryUsage().getUsed();
      sampling = false; sampler.join(1000);
      Arrays.sort(latencies);
      check(heapAfter - heapBefore < 64L * 1024 * 1024, "retained heap grew by 64 MiB");
      String result = String.format(Locale.ROOT,
          "JDBC_PERF_RESULT {\"workload\":\"%s\",\"sql_chars\":%d,\"value_chars\":%d,\"workers\":%d,\"iterations\":%d,\"operations\":%d,\"elapsed_ms\":%.3f,\"ops_per_second\":%.3f,\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f,\"max_ms\":%.3f,\"cpu_ms\":%.3f,\"heap_max_bytes\":%d,\"heap_peak_bytes\":%d,\"heap_before_gc_bytes\":%d,\"heap_after_gc_bytes\":%d,\"gc_count\":%d,\"gc_ms\":%d,\"jdk\":\"%s\",\"os\":\"%s\",\"arch\":\"%s\",\"processors\":%d}%n",
          name, workload.sql.length(), workload.value == null ? 0 : workload.value.length(), workers, iterations, latencies.length,
          elapsedMs, latencies.length * 1000 / elapsedMs, percentile(latencies,.5), percentile(latencies,.95), percentile(latencies,.99),
          percentile(latencies,1), cpuNs / 1e6, Runtime.getRuntime().maxMemory(), PEAK_HEAP.get(), heapBefore, heapAfter,
          gc(false)-gcCount, gc(true)-gcMs, System.getProperty("java.version"), System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors());
      System.out.print(result);
    } finally {
      start.countDown(); pool.shutdownNow(); sampling = false; sampler.interrupt();
    }
  }
}
