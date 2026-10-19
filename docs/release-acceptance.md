# JDBC release acceptance

Every tag and manual release requires `core-tests.yml` success for the same commit
before publication. The reusable workflow requires all three independent gates:

1. Build the shaded driver on JDK 8, run the offline contract suite, and exercise
   the candidate against the fixed MaxCompute Emulator revision. The cloud job
   needs only synthetic credentials; it uploads test reports, the candidate JAR
   and checksum, emulator source commit, image ID and capabilities.
2. Run the packaged-driver consumer matrix on JDK 8/11/17/21 and compare the
   observable results. A failed matrix blocks publication.
3. Require authenticated `jdbc-packaged-live-core` commit status for the exact
   release commit. The latest status must be successful and its creator must be
   the release operator. Missing, failed or different-commit evidence blocks
   publication. This requirement applies to both tag and manual releases.

Emulator protocol coverage includes scalar/NULL/ARRAY results, typed prepared
parameters, batch Tunnel writes and reads, STRUCT values, metadata, row limits,
SQL errors, connection reuse and SQLRT sessions. It also requires all fifteen namespace and scroll contracts
to execute with zero skips. [SQL performance regression](sql-performance.md) additionally
checks long SQL, concurrent callers and bounded JVM memory, including the default
full-parser bypass. It does not establish cloud IAM,
distributed scheduling, MaxQA v2 capacity/fallback or service-specific extended
value transfer. The default `mvn test` still runs the live service suite;
`-Pemulator-ci` selects offline contracts for reproducible cloud CI.

## Live packaged-driver acceptance

After the final commit is clean, download the candidate JAR from that commit's CI
artifact. Supply approved credentials through the component test configuration
and set non-secret `MAXCOMPUTE_ENDPOINT` and `MAXCOMPUTE_PROJECT`, then run:

```bash
bash scripts/jdbc-core-smoke.sh /path/to/odps-jdbc-<version>.jar
```

This uses only the packaged JAR and the JDK. It verifies connection, real SQL,
numeric/string/NULL results, metadata, prepared binding, result exhaustion,
resource closure and ARRAY mapping in default, explicit legacy and standard
modes, by index/label and explicit List/Array requests. Queries use constants and
modify no tables. Missing variables, SQL errors, timeouts or assertions fail; never
report success for a skipped test. Credentials must not enter arguments, records
or source control. The gate is focused live acceptance, not the entire service
integration suite.

Report the result as `jdbc-packaged-live-core` on the exact tested commit, linking
the CI artifact/acceptance record and recording the candidate checksum. A manual
Release can pass the full SHA as `local_core_sha`; it must equal `github.sha`.
Tag-triggered releases validate `github.sha` directly. Both paths run Emulator
and the JDK consumer matrix and verify the live status. Neither path obtains
cloud credentials through OIDC.

For optional MaxQA v2 service acceptance, set `MAXCOMPUTE_MAXQA_QUOTA` to a healthy
quota. Add `-Dmaxqa.required=true` when the capability is mandatory; missing quota
configuration then fails rather than skips. Three-tier tests discover the project
schema capability independently.

## Historical limitation

Version 3.10.13 predates the mandatory live gate. Its offline checks passed but
live SQL acceptance was not completed because OIDC authentication failed. Do not
describe it as live-validated, republish its immutable Maven version or move its
tag. The current cloud CI uses Emulator, while publication still requires actual
MaxCompute SQL evidence for the final candidate.
