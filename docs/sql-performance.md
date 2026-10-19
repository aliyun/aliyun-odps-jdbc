# SQL submission performance regression

The default connection uses `skipCheckIfSelect=true`. Statement SET extraction uses
`SettingParser`'s state machine; it does not call the legacy `Utils.parseSetting`
regex implementation. The SDK's full `OdpsParser` syntax-tree classification is
bypassed on this path. PreparedStatement still uses the SDK's **lexer** to locate
placeholders; a lexer and a full syntax-tree parser have different costs.

The regression runs actual JDBC calls from the shaded candidate JAR against the
fixed loopback Emulator revision used by CI. Each worker owns and closes its own
connection, statements and results. No real cloud identity or cloud capacity is
part of the benchmark.

## Run

Build the driver with JDK 8 and start the CI-pinned Emulator on port 18080 (see
`.github/workflows/emulator-tests.yml` for its source revision and Docker build).
The Emulator has a 2 GiB memory limit and 2 CPUs, `GOMEMLIMIT=512MiB`,
`GOMAXPROCS=2`, project `test_project`, 200000 rows and 512 sessions. Then run:

```bash
mvn -B -ntp clean package -Pemulator-ci
python3 scripts/jdbc-sql-performance.py target/odps-jdbc-3.10.15.jar
# More sizes and concurrency levels, using the same artifact:
python3 scripts/jdbc-sql-performance.py target/odps-jdbc-3.10.15.jar --profile baseline
```

Set `JAVA_HOME` to select the JDK. Pass `--driver-commit <full SHA>` only when the
artifact's source is verified; a JAR version alone does not prove its commit.
Reports distinguish the driver commit, JAR SHA-256, harness commit/dirty state,
JDK/OS/architecture/processor count and Emulator source/image/runtime identity.
The CI artifact `emulator-tested-driver` includes `sql-performance/results.json`,
per-process logs and thread dumps on a hard timeout.

## Cases and verdicts

- Comment-prefixed SQL from 64 KiB to approximately 1 MiB, including SET extraction.
- Token-heavy SELECT and prepared SQL with 8 simultaneous callers.
- Thousands of prepared parameters, plus wide malformed batch syntax that must
  promptly return the documented `SQLException` before any upload.
- Approximately 1 MiB string parameters with repeated unterminated comment
  markers: accepted values must round-trip unchanged. Complete block comments
  remain rejected by the existing literal heuristic, using `SQLException`.
- A small `skipCheckIfSelect=false` positive control must load the full ANTLR
  parser. Every default case must complete **without** loading it. This catches
  accidentally reintroducing full syntax-tree analysis even when a fast machine
  hides the latency impact. The control is not a large-SQL capacity guarantee for
  that explicitly enabled legacy option.

Each case runs in a fresh JVM with `-Xmx256m -Xss1m` and exits on OOM. One functional
warmup per worker precedes latency measurements. The report records operation
count, throughput, p50/p95/p99/max latency, JVM CPU time, a 10 ms sampled heap peak,
heap after explicit GC, and GC count/time. Peak sampling is approximate and is
not process RSS. End-to-end latency includes Emulator work and connection cleanup;
JVM startup and warmup are excluded from the latency samples, but included in the
separate process wall time.

CI requires complete, correct operations; no OOM, stack overflow, unexpected SQL
error, worker/JVM hang or full-parser activation on the default path. A process
has a 120 second hard deadline, p99 has a generous 20 second stability budget, and
post-warmup retained heap growth must remain below 64 MiB. These are stability
budgets, not millisecond performance promises. Compare measurements using the
same JDK, host resources, Emulator and workload; shared-runner timing is noisy.
A baseline is evidence for its recorded matrix, not a guarantee for arbitrary
SQL size or unbounded concurrency. The extended profile adds 1/16-worker cases.

## Regressions found while adding this gate

The previous 3.10.15 candidate (`9af29c91850996de6256ede51df413612dea522d`)
reproduced `StackOverflowError` through the default JDBC API with a 3000-character
string containing repeated `/*x`. Long complete comments and wide malformed
batch/partition lists also exhausted the regex stack. The fixes preserve the
previous string-check decisions, use a linear block-comment scan and possessive
comma-list matching. Offline regressions include differential short-input checks
and large accepted/rejected inputs; packaged-driver stress exercises the real
call sites. A direct full-ANTLR OOM probe is outside the default path and is not
presented as the cause of the historical incident.
