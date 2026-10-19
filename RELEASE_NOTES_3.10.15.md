# MaxCompute JDBC 3.10.15

Release candidate. Changes since 3.10.14.

## Fixes

- Update bundled Jackson to 2.21.7, LZ4 to 1.11.4, Zstandard to 1.5.7-17 and Netty to 4.1.137.Final. Dependency versions are pinned before shading.
- Return `SQLException` consistently when a connection cannot be established, preserving the original cause and MaxQA routing diagnostics.
- Correct statement/prepared-statement/result resource cleanup, cancellation and closed-object contracts. Validate prepared parameter bindings before uploading rows.
- Correct catalog/schema identity, column precision/scale and metadata filtering, including three-tier schema resolution.
- Use the configured Tunnel endpoint consistently for upload and download.

## Compatibility

Java 8 remains the minimum runtime; the SDK stays at `0.58.1-public`. The default ARRAY mapping remains `legacy_array_get_object=true`. Applications can opt into `java.sql.Array` with `legacy_array_get_object=false`; explicit typed getters work in both modes.

## Validation

The cloud CI runs offline contracts plus the shaded candidate against a fixed MaxCompute Emulator source commit. Protocol acceptance covers scalar/NULL results, typed prepared literals, batch Tunnel write/read, ARRAY/STRUCT values, metadata, row limits, error propagation, connection reuse and SQLRT sessions, plus fifteen namespace/scroll contracts with zero skips. The JDK 8/11/17/21 consumer matrix remains a separate gate.

A release also requires authenticated live core SQL acceptance for the exact candidate commit. Emulator acceptance does not establish cloud IAM, distributed scheduling, MaxQA v2 capacity/fallback or service-specific BLOB/GEOGRAPHY transfer support. The live service suite remains available without the `emulator-ci` profile; quota-specific tests require `MAXCOMPUTE_MAXQA_QUOTA`, with `-Dmaxqa.required=true` to reject a missing quota.

Maven coordinates: `com.aliyun.odps:odps-jdbc:3.10.15`.
