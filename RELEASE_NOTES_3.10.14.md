# MaxCompute JDBC 3.10.14

Release date: 2026-09-22. Changes since 3.10.13.

## Fixes

- **ARRAY access for JDBC consumers (#181).** Applications such as BI tools that expect `java.sql.Array` for columns advertised as `Types.ARRAY` can now opt into that mapping for untyped `ResultSet.getObject()` by setting the connection property `legacy_array_get_object=false`, or URL parameter `legacyArrayGetObject=false`. This avoids the `ArrayList cannot be cast to java.sql.Array` failure in those consumers.
- **Explicit ARRAY types.** `getObject(index, java.sql.Array.class)` and its column-label overload return a JDBC array in both modes. Explicit `List.class` requests continue to return the underlying list. `getArray()` remains available in both modes.
- **MaxQA routing diagnostics.** When routing fails before fallback and SQL execution subsequently fails, the `SQLException` now includes the earlier routing failure message and retains it as a suppressed exception when it is not already the cause. This preserves the execution failure while making the routing problem diagnosable.

## Compatibility and configuration

The default is **unchanged**: `legacy_array_get_object=true`, so untyped `getObject()` still returns `java.util.List` for ARRAY values. Upgrading alone does not enable standard ARRAY mapping; configure the option for consumers that require it. Existing applications casting untyped ARRAY results to `List` can keep their settings.

```java
Properties properties = new Properties();
properties.setProperty("legacy_array_get_object", "false");
// Supply the usual connection and authentication properties separately.
```

Equivalent JDBC URL option: `legacyArrayGetObject=false` (use `?` for the first parameter or `&` after existing parameters).

This patch keeps the Java 8 baseline and SDK dependency `0.58.1-public` from 3.10.13. It introduces no dependency upgrades and does not change the dependency-security assessment documented for 3.10.13. It is a release of the 3.10 master line, not a cumulative release of the separate 3.9 maintenance branch.

## Release validation

Publication requires regression tests and live core SQL acceptance against the packaged driver from the same commit. The gate covers connection, scalar/NULL results, metadata, prepared parameters and resource closure, plus ARRAY default/standard modes and explicitly requested Java types. This is focused release acceptance, not the full integration suite.

Maven coordinates: `com.aliyun.odps:odps-jdbc:3.10.14`.

[Full source comparison](https://github.com/aliyun/aliyun-odps-jdbc/compare/v3.10.13...v3.10.14)
