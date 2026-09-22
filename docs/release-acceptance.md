# JDBC release acceptance

Every tag release and manual release runs `core-tests.yml` for the same commit before the publishing job. The `release` job requires `core-tests` success. A failed build, missing credentials, STS/OIDC error, timeout or failed assertion blocks publication; no skipped-test fallback is allowed.

The gate builds the shaded driver on JDK 8, runs dependency regression tests, and executes `scripts/JdbcCoreSmoke.java` using only that packaged JAR and the JDK. Against the regression project it verifies connection, real `Statement.executeQuery`, numeric/string/NULL results, metadata, prepared parameter binding, result exhaustion and resource closure. Queries use constants; no tables or user data are modified. It is the minimum release gate, not the complete JDBC integration suite.

To run the same gate locally, provide approved credentials through the credential broker and set non-secret `MAXCOMPUTE_ENDPOINT` and `MAXCOMPUTE_PROJECT`, then run `bash scripts/jdbc-core-smoke.sh target/odps-jdbc-<version>.jar`. Missing variables exit nonzero. Never put credentials in arguments or checked-in files.

The existing broader integration CI also propagates Maven test failure. Its test report does not replace the core acceptance gate.

## 3.10.13 historical limitation

Version 3.10.13 was published before this mandatory gate was introduced. Its JDK 8 build/dependency tests and downloaded-JAR offline smoke passed, but live SQL acceptance was not completed: GitHub STS failed with `AuthenticationFail.OIDCToken.PublicKeyFingerprintMismatch`. This release must not be described as having passed live core SQL tests. Fix the existing RAM OIDC trust configuration and rerun the gate before the next release. Do not republish the existing immutable Maven version or move its tag.

## Local acceptance route

When OIDC is unavailable, a release operator may run the same packaged-driver live gate locally using the approved component test configuration. After testing the final, clean commit, the operator reports the result as the `jdbc-packaged-live-core` GitHub commit status, including a link to the acceptance record. Never report success for a skipped, failed, timed-out or different-commit test.

Dispatch Release with `local_core_sha` set to that full commit SHA. The alternative core job builds the same commit on JDK 8 and runs all release regression tests, then requires the input to equal `github.sha`, the latest live status to be successful, and its creator to equal the release operator. Missing or failed evidence blocks publication. Default tag-triggered releases continue to execute live SQL through OIDC. Both paths remain required by the publishing job through `needs: core-tests`.

The packaged-driver gate also checks ARRAY mapping in default, explicit legacy and standard modes, by index and label, with explicit List and Array requests. Local execution may use JDK 17 with the driver's Java 8 bytecode target; the hosted build independently validates compilation and regression tests on JDK 8. This route avoids storing local test credentials in GitHub.
