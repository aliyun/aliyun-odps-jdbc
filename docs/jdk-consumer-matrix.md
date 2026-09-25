# JDK consumer compatibility matrix

A JDBC driver runs inside a JVM that the application owner picks, not the JVM that produced the
jar. This driver is compiled with `-source/-target 1.8`, and the existing CI jobs build and test on
JDK 8 only. `scripts/jdk-consumer-matrix.sh` + `scripts/JdkConsumerMatrix.java` measure what a
consumer actually sees when the same packaged jar is dropped into a different JVM, and the
`JDK consumer matrix` workflow runs those measurements on JDK 8, 11, 17 and 21.

## What it measures

The probe uses the driver the way an application does: the packaged jar is the only library on the
classpath and the entry point is `java.sql.DriverManager`. Each observable is printed as a
`key=value` line; the runner compares those lines across JVMs.

| Cell | Needs credentials | Covers |
| --- | --- | --- |
| `classloading` | no | service-file registration without `Class.forName`, driver version/`acceptsURL`/`getPropertyInfo`, linking the driver classes and the relocated third-party code that exists only in the shaded jar, static initialization of the SDK and relocated Arrow |
| `core-contract` | no | a real `Connection` object against a closed loopback port, `DatabaseMetaData` values, statement creation, the exception type of a failing execute |
| `resource-close` | no | `close()`/`isClosed()`/use-after-close for `Statement`, `PreparedStatement` and `Connection`, plus which non-daemon threads survive |
| `live-sql` | yes | connect, `Statement.executeQuery`, `PreparedStatement` binding, `DatabaseMetaData.getColumns`, thread accounting after statement reuse, partial read, and a scrollable result set |

The credential-free cells exist so that a fork can reproduce the gate and so that service
availability cannot make it red. They are not a substitute for live SQL acceptance, which stays in
`core-tests.yml` and `docs/release-acceptance.md`. A live cell without credentials reports
`live_sql=SKIPPED:no-credentials-in-environment`; it never reports a pass.

The offline cells do not fake a server. `timezone`, `odpsNamespaceSchema=false` and
`useInstanceTunnel=false` are the three connection switches that stop the driver constructor from
reading project settings, reading the session time zone and looking up the tunnel endpoint, so the
real `OdpsConnection` gets built with nothing listening on the other end.

## How to run

```bash
mvn -B -ntp clean package -DskipTests
bash scripts/jdk-consumer-matrix.sh target/odps-jdbc-<version>.jar \
  /usr/lib/jvm/java-8-openjdk-amd64 \
  /usr/lib/jvm/java-11-openjdk-amd64 \
  /usr/lib/jvm/java-17-openjdk-amd64 \
  /usr/lib/jvm/java-21-openjdk-amd64
```

Give JDK homes, not JRE homes: each JVM also compiles the probe against the packaged jar, because
compiling an application against the driver is part of the consumer experience. With no JVM
arguments the script uses the ambient `java`/`javac`, which is what a CI matrix cell wants.

For the live cells, export the same four variables `scripts/jdbc-core-smoke.sh` uses
(`ALIBABA_CLOUD_ACCESS_KEY_ID`, `ALIBABA_CLOUD_ACCESS_KEY_SECRET`, `MAXCOMPUTE_PROJECT`,
`MAXCOMPUTE_ENDPOINT`) through your credential broker. Never put credentials in arguments or
checked-in files. `MATRIX_TIMEOUT` (default 300 s) bounds one cell;
`MATRIX_RESULTS_DIR` keeps the raw `key=value` observables for inspection.

Exit status is 0 only when every cell terminated by itself, no check failed, and no compared
observable differs between JVMs. `runtime_*` and latency buckets are excluded from the comparison.

## Interpretation rules that were learned the hard way

* **A JVM that does not exit is a failure, not a slow pass.** The probe never calls `System.exit`
  on the happy path, so a leftover non-daemon thread surfaces as a cell timeout. Note how the two
  mechanisms divide the work: the in-probe check compares against a per-scenario baseline and
  therefore will not blame a thread that existed before the first cell, while the runner's timeout
  catches anything at all that keeps the JVM alive. Both fire on the same class of bug, and the
  timeout is the one that cannot be fooled by baseline choice.
* **Idle pool threads are not a leak.** Worker threads of a bounded pool expire after the pool's
  keep-alive. The REST client in the packaged jar builds
  `new ThreadPoolExecutor(0, 3, 10L, SECONDS, ...)` for its deprecation logging (visible in the
  bytecode of `com/aliyun/odps/rest/RestClient.class` inside the jar), so a check that looks 6
  seconds after close reports `pool-N-thread-N` threads that were already on their way out. My
  first version of this matrix read that as a leak. It now waits 20 seconds.
* **Attribution is per scenario.** Thread accounting uses a thread-identity baseline taken before
  each scenario, otherwise one scenario's threads get blamed for the next one.
* **Values that embed a port, endpoint or instance id are reduced to the exception type.** Nothing
  in this output is safe to paste into a bug report otherwise.

## Findings recorded while building the matrix (2026-09-25, driver 3.10.14)

These were identical on JDK 8, 11, 17 and 21, so they are not JVM differences. The probe reports
them as `FINDING` lines instead of failing the cell, because a JVM-independent defect belongs to
its own issue and a permanently red gate stops being a gate. The parity comparison still catches
them if they ever become JVM-specific.

1. `Statement` and `PreparedStatement` use-after-close throws `NullPointerException` rather than
   `SQLException`. Minimum reproduction, no server needed: open an offline connection (the three
   switches above against a closed port), `statement.close()`, then `statement.executeQuery(...)`.
   JDBC requires `SQLException` on a closed `Statement`. `Connection` use-after-close already throws
   `SQLException`, so only the statement classes are affected.
2. Relocated Arrow classes (`com.aliyun.odps.jdbc.shaded.org.apache.arrow.*`) initialize through
   `org.slf4j`, which the build keeps in `provided` scope on purpose and therefore does not put in
   the shaded jar. An application that runs the packaged jar alone and reaches an Arrow code path
   gets `NoClassDefFoundError: org/slf4j/LoggerFactory`. Keeping `slf4j-api` on the classpath
   avoids it; the driver's default instance-tunnel read path does not initialize Arrow, which is
   why this does not show up in ordinary query use.

## The gate fails on purpose

Verified by mutating the probe (and reverting the mutation), not by reasoning about it:

* Make one observable differ on JDK 21 while JDK 8 keeps its value, and the runner prints
  `INCOMPATIBILITY meta_product: jdk-0-java-8-openjdk-amd64=MaxCompute/ODPS/0.58.1-public
  jdk-1-java-21-openjdk-amd64=Mutated/For-Divergence-Test`, `== matrix rc=1`.
* Start a non-daemon thread that never returns and run one JVM: the runner prints
  `TIMEOUT jdk-0-java-21-openjdk-amd64: JVM still alive after 25s; a non-daemon thread is keeping
  a consumer JVM running` and exits 1.

## Current result

Measured on 2026-09-25 against the jar produced by `mvn clean package -DskipTests` at this commit,
one JVM per cell, same 57 MB packaged driver in every cell:

| JVM (Ubuntu 24.04 OpenJDK packages) | Offline cells | Live SQL cells | Parity vs JDK 8 cell |
| --- | --- | --- | --- |
| 1.8.0_502 | PASS | PASS | baseline |
| 11.0.32.1 | PASS | PASS | identical |
| 17.0.20 | PASS | PASS | identical |
| 21.0.12 | PASS | PASS | identical |

Live SQL ran read-only against the regression project reached by the component test credentials —
constant-value queries plus `DatabaseMetaData.getColumns`, no table created, altered or dropped.
The CI workflow has no credentials, so its live cells report `SKIPPED`; the four-JVM live result
above comes from the local run: the same script, four JDK homes, credentials injected through the environment.

After excluding the JVM-identifying keys (`runtime_*`) and latency buckets, the JDK 8 cell and each
other cell are byte-for-byte identical, offline and live. The workflow itself was verified on pull
request: package job plus the four consumer cells all green, each cell printing
`live_sql=SKIPPED:no-credentials-in-environment`.

## Scope and non-claims

The matrix says: the jar built from this commit, on the JVMs listed above, behaves identically for
the measured observables. It does not certify every JDBC method on every JVM, it does not cover
Android or IBM J9/OpenJ9, and it does not change the compiled target, which stays `1.8`.
