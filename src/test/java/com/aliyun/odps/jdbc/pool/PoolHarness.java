/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.jdbc.pool;

import com.aliyun.odps.jdbc.OdpsConnection;

import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared plumbing for the connection-pool consumer regressions.
 *
 * <p>A pool is only interesting to a driver for the calls it makes on a connection it did not
 * open: validation on re-use, state reset on return, close on eviction. Those calls have to be
 * observed, so the harness pins down (a) an endpoint that behaves like a MaxCompute service that
 * is unreachable, (b) the physical connection behind a pool proxy, and (c) the threads a pool
 * leaves behind.
 */
final class PoolHarness {

  /** Pool names are unique so that thread accounting cannot be confused by another pool. */
  private static final AtomicInteger POOL_SEQ = new AtomicInteger();

  private PoolHarness() {
  }

  static String nextPoolName() {
    return "odps-jdbc-pool-test-" + POOL_SEQ.incrementAndGet();
  }

  /**
   * A JDBC URL whose endpoint is provably not listening.
   *
   * <p>Reserving and releasing a {@link ServerSocket} yields a port that nothing owns, so opening
   * a connection can fail fast instead of hanging on a firewall timeout. The driver still has to
   * be told not to talk to the service while it is being constructed: {@code timezone} and
   * {@code odpsNamespaceSchema} keep the constructor from reading project properties, and
   * {@code useInstanceTunnel=false} keeps {@code SQLExecutor} from resolving a tunnel endpoint.
   * Without those, "offline" would quietly turn into "three REST calls to a dead port".
   */
  /** The project the harness URL names; a fresh connection reports it as its catalog. */
  static final String PROJECT = "pool_lifecycle_project";

  static String deadEndpointUrl() throws IOException {
    int port;
    try (ServerSocket reserved = new ServerSocket(0)) {
      port = reserved.getLocalPort();
    }
    return "jdbc:odps:http://127.0.0.1:" + port + "/api?project=" + PROJECT
           + "&accessId=pool_test_access_id&accessKey=pool_test_access_key"
           + "&timezone=Asia/Shanghai&odpsNamespaceSchema=false&useInstanceTunnel=false"
           + "&interactiveMode=false&connectTimeout=2000&readTimeout=2000";
  }

  /**
   * The driver connection underneath a pool proxy.
   *
   * <p>{@code HikariCP} hands out its own proxy; everything a consumer can do to the physical
   * connection behind its back (close it, read the state a previous borrower left) goes through
   * this unwrap.
   */
  static OdpsConnection physical(Connection pooled) throws Exception {
    return pooled.unwrap(OdpsConnection.class);
  }

  /**
   * Statements the connection is still tracking.
   *
   * <p>Package-private field, read reflectively on purpose: a leaked handle list is invisible
   * through the JDBC API, which is exactly why a pool can carry one for the life of the process.
   */
  static int trackedStatementCount(OdpsConnection physical) throws Exception {
    java.lang.reflect.Field field = OdpsConnection.class.getDeclaredField("stmtHandles");
    field.setAccessible(true);
    Object handles = field.get(physical);
    synchronized (handles) {
      return ((List<?>) handles).size();
    }
  }

  /**
   * The same service identity the rest of the suite uses, taken from the environment the CI test
   * job injects -- see {@code utils.TestUtils#getConnection}. Nothing is hardcoded here.
   *
   * @return a JDBC URL for the live project, or {@code null} when this machine has no test identity
   */
  static String liveUrl() {
    String endpoint = System.getenv("odps_endpoint");
    String project = System.getenv("MAXCOMPUTE_PROJECT");
    String accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    String accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
    if (StringUtils.isBlank(endpoint) || StringUtils.isBlank(project)
        || StringUtils.isBlank(accessId) || StringUtils.isBlank(accessKey)) {
      return null;
    }
    String stsToken = System.getenv("ALIBABA_CLOUD_SECURITY_TOKEN");
    String url = "jdbc:odps:" + endpoint + "?project=" + project
                 + "&accessId=" + accessId + "&accessKey=" + accessKey + "&enableLimit=false";
    if (!StringUtils.isBlank(stsToken)) {
      url += "&stsToken=" + stsToken.replace("  ", " ");
    }
    return url;
  }

  /**
   * Condition method for {@code @DisabledIf}: skip with a visible reason rather than letting an
   * absent identity look like a driver failure.
   */
  static boolean noLiveService() {
    return liveUrl() == null;
  }

  /** Names of live threads whose name carries the pool name, including daemon ones. */
  static List<String> livePoolThreads(String poolName) {
    List<String> names = new ArrayList<>();
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      if (thread != null && thread.getName() != null && thread.getName().contains(poolName)) {
        names.add(thread.getName());
      }
    }
    return names;
  }

  /** @return the pool's threads after giving an orderly shutdown a chance to finish */
  static List<String> livePoolThreadsAfter(String poolName, long graceMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + graceMillis;
    List<String> live = livePoolThreads(poolName);
    while (!live.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(50L);
      live = livePoolThreads(poolName);
    }
    return live;
  }
}
