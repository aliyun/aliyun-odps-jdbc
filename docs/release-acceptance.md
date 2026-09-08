# JDBC release acceptance

Every tag release and manual release runs `core-tests.yml` for the same commit before the publishing job. The `release` job requires `core-tests` success. A failed build, missing credentials, STS/OIDC error, timeout or failed assertion blocks publication; no skipped-test fallback is allowed.

The gate builds the shaded driver on JDK 8, runs dependency regression tests, and executes `scripts/JdbcCoreSmoke.java` using only that packaged JAR and the JDK. Against the regression project it verifies connection, real `Statement.executeQuery`, numeric/string/NULL results, metadata, prepared parameter binding, result exhaustion and resource closure. Queries use constants; no tables or user data are modified. It is the minimum release gate, not the complete JDBC integration suite.

To run the same gate locally, provide approved credentials through the credential broker and set non-secret `MAXCOMPUTE_ENDPOINT` and `MAXCOMPUTE_PROJECT`, then run `bash scripts/jdbc-core-smoke.sh target/odps-jdbc-<version>.jar`. Missing variables exit nonzero. Never put credentials in arguments or checked-in files.

The existing broader integration CI also propagates Maven test failure. Its test report does not replace the core acceptance gate.

## 3.10.13 historical limitation

Version 3.10.13 was published before this mandatory gate was introduced. Its JDK 8 build/dependency tests and downloaded-JAR offline smoke passed, but live SQL acceptance was not completed: GitHub STS failed with `AuthenticationFail.OIDCToken.PublicKeyFingerprintMismatch`. This release must not be described as having passed live core SQL tests. Fix the existing RAM OIDC trust configuration and rerun the gate before the next release. Do not republish the existing immutable Maven version or move its tag.
