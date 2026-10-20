# JDBC 3.9 release acceptance

The compatibility line requires both gates in `core-tests.yml` before publishing:

- Build the assembly driver on JDK 8 and run typed-result, MaxQA fallback, result lifecycle, affected-row count, iterator close, settings and utility regressions with zero failures.
- Require the latest authenticated `jdbc-packaged-live-core` commit status to be successful for the exact release commit, created by the release operator. A missing, failed or different-commit status blocks publication.

Download the candidate JAR from that commit's CI artifact. Supply approved test credentials through the component test configuration, set `MAXCOMPUTE_PROJECT` and `MAXCOMPUTE_ENDPOINT`, and run:

```bash
bash scripts/jdbc-core-smoke.sh /path/to/odps-jdbc-<version>-jar-with-dependencies.jar
```

This exercises the packaged driver against actual MaxCompute SQL, including connection, result reads, metadata, bound parameters and closure. Constant queries modify no tables. Record the exact commit, artifact SHA-256 and execution result before reporting the authenticated commit status. Never put credentials in source, command arguments or records. The tag release runs both gates again; a manual release may pass `local_core_sha`, which must equal the workflow commit.

The compatibility line retains its existing assembly packaging and regression scope. The 3.10 Emulator/JDK consumer matrix and standard java.sql.Array support have not been backported. Core SQL acceptance does not establish live MaxQA v2 quota/fallback behavior. Published Maven versions and release tags are immutable; a failed upload requires staging inspection before any retry.
