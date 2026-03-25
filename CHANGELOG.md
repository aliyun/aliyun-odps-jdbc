# Changelog

## [3.10.3] - 2026-03-25
### Bug Fixes
- **Data Iterator Concurrency & Resource Management**
  Fixed concurrency safety issues and resource release problems in `InstanceDataIterator`:
  - Added `AtomicBoolean` to control close state and prevent concurrent access
  - Integrated `OdpsLogger` for error and warning logging
  - Removed `synchronized` keyword from `submitNextSplit` method to improve concurrent performance
  - Implemented graceful thread pool shutdown and queue cleanup in `close` method
  - Fixed resource leak issues in `ResultSet` constructor
  - Ensured proper `ResultSet` closure after `Statement` execution

---

## [3.10.2] - 2025-11-12
### Bug Fixes
- **Catalog and Schema Query Logic**
  Fixed directory and schema query logic to correctly handle namespace mode and three-tier model scenarios in `OdpsDatabaseMetaData`.
- **Catalog and Schema Get/Set Methods**
  Corrected the retrieval and setting methods for Catalog and Schema operations in `OdpsConnection` and related classes.
- **Connection Namespace Mode Property Key**
  Updated the property key for namespace mode configuration from incorrect value to proper connection settings.

---

## [3.10.1] - 2025-09-01
### Improvements
- **Dependency Shading Optimization**
  Excluded `netty-arrow` from shading exclusion list to prevent Arrow-Netty compatibility issues.

---

## [3.10.0] - 2025-08-21
### New Features
- **ARRAY Type Support**
  Full support for MaxCompute ARRAY type with new `OdpsArray` class implementing `java.sql.Array` interface:
  - Implemented `getBaseTypeName()`, `getBaseType()`, `getArray()` methods
  - Support for array data transformation and type conversion
  - Compatible with JDBC standard array operations
  
- **MaxQA v2 Integration**
  Integrated MaxQA v2 connection support with comprehensive improvements:
  - Added `MaxQAConnInfo` class for dedicated MaxQA connection information handling
  - Removed dynamic quota switching via SET clause in favor of explicit connection configuration
  - Optimized WLM quota fetching mechanism to avoid unnecessary project property access
  - Updated SDK version to `0.57.x` for MaxQA v2 compatibility

### Changes
- **Dependency Relocation**
  Comprehensive dependency relocation to prevent classpath conflicts:
  - All third-party dependencies shaded under `com.aliyun.odps.jdbc.shaded.*` namespace
  - Includes ANTLR, Netty, Jackson, Guava, Commons, Arrow, Protobuf, and other libraries
  - Excluded SLF4J from shading to maintain logging compatibility
  
- **Testing Framework Migration**
  Migrated from JUnit 4 to JUnit 5:
  - Updated test annotations and assertions to modern JUnit Jupiter API
  - Improved test organization and preparation code with timezone settings
  
- **SDK Version Update**
  Upgraded `odps-sdk` to version `0.57.0-public`

---

## [3.9.4] - 2025-07-31
### New Features
- **OffsetDateTime Support**
  `PreparedStatement` now supports `OffsetDateTime` type when setting `Timestamp` parameters.
- **Logview Configuration Fix**
  Fixed issue where `logviewHost` setting was not being applied correctly.

---

## [3.9.3] - 2025-06-04
### New Features
- **Enhanced Interactive Mode**
  The `interactMode` parameter has been converted from Boolean to Enum type with optional values: `Offline`, `MCQA`, and `MaxQA`, providing more granular control over execution modes.

### Bug Fixes
- **Settings Parser Semicolon Handling**
  Fixed SettingsParser to always terminate with a semicolon for proper SQL statement parsing.

---

## [3.9.2] - 2025-05-08
### New Features
- **SQL Validation Skip Parameter**
  Added new parameter `skipCheckIfSelect` to bypass SELECT statement validation when needed.

### Bug Fixes
- **SQL Hints with Timezone**
  Fixed issue where SQL hints were incorrectly set when specifying timezone configurations.
- **Tunnel Error Workaround**
  Implemented workaround for Tunnel errors when using `InstanceDataIterator`.

---

## [3.9.1] - 2025-04-22
### New Features
- **WLM Quota Configuration**
  Added support for setting `odps.task.wlm.quota` parameter for workload management quota configuration.
- **Multi-thread Download Support**
  Enabled multi-threaded download capability for improved data retrieval performance.

---

## [3.9.0] - 2024-12-02
### New Features
- **Timezone-Aware Time Types**
  Enhanced time type handling to support specified timezones. Improved getter methods for better compatibility across different timezone configurations.
- **ZonedDateTime Conversion**
  Properly handled `ZonedDateTime` to `Timestamp` conversion for accurate temporal data representation.
- **Default Timezone and Null Value Parsing**
  Added support for default timezone values and improved null value parsing in JDBC operations.

### Bug Fixes
- **Three-Tier Model Settings Configuration**
  Fixed settings configuration for three-tier model (project.schema.table) scenarios in `OdpsConnection`.

---

## [3.8.5] - 2025-03-18
### Improvements
- **Dependency Update**
  Upgraded `odps-sdk` to version `0.51.11-public`. The new SDK version refactors the `MCQA 2.0 ResultDescriptor` entity to ensure compatibility with MCQA 2.0. Users of MCQA 2.0 are **strongly recommended** to upgrade to this version. For details, refer to the [SDK Changelog](https://github.com/aliyun/aliyun-odps-java-sdk/releases/tag/v0.51.11-public).
---

## [3.8.3] - 2025-02-11
### Enhancements
- **SQL Settings Extraction**
  Replaced regular expressions with a state machine for SQL settings parsing, improving accuracy and resolving issues where JDBC-side SQL rewriting caused execution failures.
- **Dependency Update**
  Upgraded `odps-sdk` to version `0.51.6-public`.
---

## [3.8.2] - 2025-01-15
### Features
- **Execution Mode**
  Added `getExecuteMode` method in `OdpsStatement` to retrieve the current execution mode (`INTERACTIVE`, `INTERACTIVE_V2`, or `OFFLINE`).
- **Dependency Update**
  Upgraded `odps-sdk` to version `0.51.5-public`.
---

## [3.8.1] - 2024-11-27
### Features
- **Logview** Added support for Logview V2, details see [November 14, 2024 (UTC+8): Notice on the security upgrade for MaxCompute LogView](https://www.alibabacloud.com/help/en/maxcompute/product-overview/2024-service-notices). specifies the version through the url param `logviewVersion`.
  method.

----
## [3.8.0] - 2024-10-09
**Welcome to the MCQA 2.0 Era!**
### New Features
- Support for submitting MCQA 2.0 jobs. Enable MCQA 2.0 mode by setting `quotaName` to interactive quota and turning on `interactiveMode=true`.
- New parameter `useInstanceTunnel`, which specifies whether to use Tunnel to retrieve data. The default value is true, consistent with previous behavior.
  If `instanceTunnel` is turned off, the system will use RESTful API to retrieve data under this mode:
    - Execution speed will be faster.
    - Only 10,000 results can be returned; if this limit is exceeded, the data will be truncated.
    - Return values are of String type, suitable only for display-type jobs.
- New executable class `JdbcRunner`, which allows executing JDBC tasks via a connection string and SQL file. The usage is as follows:
```bash
 java -cp odps-jdbc-version-jar-with-dependencies.jar com.aliyun.odps.jdbc.JdbcRunner <jdbc_url> <sql_file>
```
- Added some debug settings that modify the current `Connection` configuration (effectively re-acquiring the Connection to replace the current one).
    - `set tunnelEndpoint = xxx;` modifies the tunnelEndpoint parameter of the current Connection.
    - `set useTunnel = true/false;` modifies the useInstanceTunnel parameter of the current Connection.
    - `set interactiveMode = true/false;` modifies the interactiveMode parameter of the current Connection.
### Changes
- Removed handling of `#` comment in the `removeComment` method: it no longer removes comments after the `#`. This change resolves many issues where valid SQL statements were incorrect due to the removal of `#`.
- Updated odps-sdk version to [0.50.0-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG.md#0500-public---2024-10-09).

## [3.7.0] - 2024-08-29
**Compatibility with Metabase!**  
[Learn how to connect MaxCompute to Metabase](https://help.aliyun.com/zh/maxcompute/user-guide/connect-metabase-to-maxcompute)
### Important Changes
- **Optimized the `DatabaseMetadata.getColumns` Logic**: Enhanced compatibility for three-tier and two-tier model users as follows:
    - **Three-tier model (project.schema.table)**:
        - Ignores the passed `catalog`, using the project name from the JDBC connection, along with the `schemaPattern` and `tableNamePattern` parameters to compose the table reference.
    - **Two-tier model (project.table)**:
        - Ignores the `schemaPattern`, treating the `catalog` as the project name, and combines it with the `tableNamePattern` parameter to form the table reference.
  ```text
  Three-tier model: catalog.schemaPattern.tableNamePattern -> project.schemaPattern.tableNamePattern
  Two-tier model: schemaPattern.tableNamePattern -> catalog.tableNamePattern
  ```
### Changes
- **Enhanced SQLException Messages**: The exception message now includes the correct ErrorMessage.
- **Updated odps-sdk Version**: Updated to [0.48.8-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG.md#0488-public---2024-08-12).

## [3.6.0] - 2024-08-07
**Say No to SQLFeatureNotSupportedException!**
### New Features
- **LocalDate Support**: `PrepareStatement.setObject` now supports LocalDate type for MaxCompute DATE type.
- **SQL Injection Check**: New parameter `skipSqlInjectCheck` allows skipping the SQL injection check, applicable when setting String type fields.
- **TIMESTAMP_NTZ Support**: `DatabaseMetadata` now also supports MaxCompute `TIMESTAMP_NTZ` type fields.
- **Extended `PrepareStatement` Methods**:
    - Implemented `setObject(int, Object, int)`, `setObject(int, Object, int, int)`, `setObject(int, Object, SQLType)`, and `setObject(int, Object, SQLType, int)` methods, which will no longer throw exceptions but will return results from `setObject(int, Object)`, ignoring additional fields.
- **Extended `ResultSet` Methods**:
    - Added `getObject(int, Map)` and `getObject(String, Map)` methods, which no longer throw exceptions but ignore the `map` and call `ResultSet.getObject(int)` to return results.
    - Added `getObject(int, Class<T>)` and `getObject(String, Class<T>)` methods, which first call `ResultSet.getObject(int)` before trying to convert the result to the specified type.
- **Optimized `Statement` Method**: The `createStatement(int, int, int)` method now no longer throws exceptions but ignores `resultSetHoldability`, returning the result of `Statement.createStatement(int, int)`.
### Changes
- **Enhanced `DatabaseMetadata.getColumns`**: The interface now returns `IS_AUTOINCREMENT` and `IS_GENERATEDCOLUMN` fields to comply with JDBC specifications.
- **Updated odps-sdk Version**: Updated to [0.48.7-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG.md#0487-public---2024-08-07).
    - The new version applies key-path-end optimizations, improving efficiency during offline job execution, especially in complex queries.

## [3.5.8] - 2024-07-22
### New Features
- **Delta Table Write Support**: `PrepareStatement` now supports write operations on Delta Tables.
- **Skip SQL Rewrite**: New parameter `skipSqlRewrite`, with a default value of `true`, allows skipping SQL statement rewriting during comment removal. Enabling this parameter may cause settings submitted with the SQL to become ineffective.
### Changes
- **Optimized Comment Removal Logic**: The logic has been optimized to avoid throwing exceptions when processing very long queries. It fixes the issue of potentially removing SQL hints during comment removal.
- **SDK Update**: The odps-sdk has been updated to [0.48.6-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG.md#0486-public---2024-07-17).
    - The new version reduces network overhead, slightly improving efficiency during offline job execution.

## [3.5.7] - 2024-04-29
### New Features
- **New Parameter `tunnelDownloadUseSingleReader`**: The default value is `false`. When this parameter is enabled, each ResultSet will use only a single Reader for data downloading. This change is suitable for scenarios that require reading large amounts of data at once, optimizing performance. The previous behavior was to open a new Reader each time `hasNext` is called, to prevent connection drops.

## [3.5.6] - 2023-10-27
### New Features
- Support for `insert into tablename (co1, co2) values(?, ?)` syntax.
- `DatabaseMetadata.getColumns` now adapts to situations using the three-tier model.