# 更新日志

## [3.8.5] - 2025-03-18
### 改进
- **依赖升级**  
  升级 `odps-sdk` 版本到 `0.51.11-public`。新版本 SDK 针对 `MCQA 2.0 ResultDescriptor` 实体进行了重构，以确保与 MCQA 2.0 的兼容性。**强烈建议 MCQA 2.0 用户升级到该版本**。详情请参考 [SDK 更新日志](https://github.com/aliyun/aliyun-odps-java-sdk/releases/tag/v0.51.11-public)。

---

## [3.8.3] - 2025-02-11
### 优化
- **SQL 设置提取**  
  使用状态机替代正则表达式进行 SQL 设置提取，提升解析准确性，规避了因 JDBC 侧 SQL 重写导致的执行失败问题。
- **依赖升级**  
  升级 `odps-sdk` 版本到 `0.51.6-public`。

----

## [3.8.2] - 2025-01-15
### 功能
- **执行模式**  
  在 `OdpsStatement` 类中新增 `getExecuteMode` 方法，用于获取当前执行模式（`INTERACTIVE`、`INTERACTIVE_V2` 或 `OFFLINE`）。
- **依赖升级**  
  升级 `odps-sdk` 版本到 `0.51.5-public`。
----
## [3.8.1] - 2024-11-27
### 功能
- **Logview** 新增对 Logview V2 的支持，V2 版本保障了数据安全，更多信息参考 [2024年11月14日-MaxCompute Logview安全升级](https://help.aliyun.com/zh/maxcompute/product-overview/2024-service-notices) 。通过url参数 `logviewVersion` 方法指定。
----

## [3.8.0] - 2024-10-9
**欢迎进入 MCQA 2.0 时代！**

### 新功能
- 支持提交 MCQA 2.0 作业。通过指定 `quotaName` 为交互式 quota，并开启 `interactiveMode=true` 即可开启 MCQA 2.0 模式。
- 新增参数 `useInstanceTunnel`，用于指定是否使用 Tunnel 来获取数据，默认值为 true，与原行为一致。

    如关闭 instanceTunnel，则会使用 Restful API 来获取数据，在这个模式下
  - 执行速度会更快。
  - 只能返回 10000 条结果，如超出这个限制，数据会被截断。
  - 返回值均为String类型，仅适合屏显型作业。
- 新增 `JdbcRunner` 可执行类，可以通过链接串和SQL文件，执行JDBC任务。使用方式如下
```bash
 java -cp odps-jdbc-version-jar-with-dependencies.jar com.aliyun.odps.jdbc.JdbcRunner <jdbc_url> <sql_file>
```
- 新增一些调试用 settings，这些 setting 会修改当前 `Connection` 的配置（相当于重新获取一次 Connection 并替代当前的 Connection）。
  - `set tunnelEndpoint = xxx;` 修改当前 Connection 的 tunnelEndpoint 参数
  - `set useTunnel = true/false;` 修改当前 Connection 的 useInstanceTunnel 参数
  - `set interactiveMode = true/false;` 修改当前 Connection 的 interactiveMode 参数

### 变更
- 移除 removeComment 方法中的 # 注释处理：现已不再去除 # 后的注释，这一改动解决了许多正常 SQL 语句因 # 而被误删的问题。
- 更新 odps-sdk 版本到 [0.50.0-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG_CN.md#0500-public---2024-10-09)


## [3.7.0] - 2024-08-29
**兼容 Metabase!**

[了解如何连接 MaxCompute 与 Metabase](https://help.aliyun.com/zh/maxcompute/user-guide/connect-metabase-to-maxcompute)

### 重要变更
- **优化 `DatabaseMetadata.getColumns` 逻辑**：增强了三层模型和两层模型使用者的兼容性，具体如下：
  - **三层模型（project.schema.table）**：
    - 忽略传入的 `catalog`，使用 JDBC 链接中的项目名，以及参数中的 `schemaPattern` 和 `tableNamePattern` 组合表引用。
  - **两层模型（project.table）**：
    - 忽略 `schemaPattern`，将 `catalog` 视为项目名，并结合参数中的 `tableNamePattern` 组成表引用。

  ```text
  三层模型：catalog.schemaPattern.tableNamePattern -> project.schemaPattern.tableNamePattern
  两层模型：schemaPattern.tableNamePattern -> catalog.tableNamePattern
  ```

### 变更
- **增强 SQLException 异常信息**：异常信息中新增了正确的 ErrorMessage。
- **更新 odps-sdk 版本**：已更新至 [0.48.8-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG_CN.md#0488-public---2024-08-12)。


## [3.6.0] - 2024-08-07
**拒绝 SQLFeatureNotSupportedException！**

### 新增功能
- **LocalDate 支持**：`PrepareStatement.setObject` 现支持 LocalDate 类型，用于 MaxCompute 的 DATE 类型。
- **SQL 注入检查**：新增参数 `skipSqlInjectCheck`，可跳过对 SQL 注入的检查，适用于设置 String 类型字段时。
- **TIMESTAMP_NTZ 类型支持**：`DatabaseMetadata` 现增加对 MaxCompute `TIMESTAMP_NTZ` 类型字段的支持。
- **扩展 `PrepareStatement` 方法**：
  - 实现了 `setObject(int, Object, int)`、`setObject(int, Object, int, int)`、`setObject(int, Object, SQLType)` 和 `setObject(int, Object, SQLType, int)` 方法，现不再抛出异常，而是返回 `setObject(int, Object)` 的结果，忽略多余字段。
- **扩展 `ResultSet` 方法**：
  - 新增 `getObject(int, Map)` 和 `getObject(String, Map)` 方法，现不抛出异常，而是忽略 `map`，调用 `ResultSet.getObject(int)` 返回结果。
  - 新增 `getObject(int, Class<T>)` 和 `getObject(String, Class<T>)` 方法，首先调用 `ResultSet.getObject(int)`，然后尝试将结果转换为指定类型。
- **优化 `Statement` 方法**：`createStatement(int, int, int)` 方法现在不再抛出异常，而是忽略 `resultSetHoldability`，返回 `Statement.createStatement(int, int)` 的结果。

### 变更
- **增强 `DatabaseMetadata.getColumns`**：接口返回值新增 `IS_AUTOINCREMENT` 和 `IS_GENERATEDCOLUMN` 字段，以符合 JDBC 规范。
- **更新 odps-sdk 版本**：已更新至 [0.48.7-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG_CN.md#0487-public---2024-08-07)。
  - 新版本应用了 key-path-end 优化，提高了在执行离线作业时的效率，在较复杂query中提升明显。

## [3.5.8] - 2024-07-22

### 新增功能
- **Delta Table 写入支持**：`PrepareStatement` 现支持对 Delta Table 的写入操作。
- **跳过 SQL 重写**：新增参数 `skipSqlRewrite`，默认值为`true`，可在去除注释过程中跳过对 SQL 语句的重写。启用该参数可能导致与 SQL 一起提交的 settings 失效。

### 变更
- **优化去除注释逻辑**：在处理非常长的查询时，已优化逻辑以避免抛出异常。修复了在去除注释的过程中，可能将 SQL hints 也去掉的问题。
- **SDK 更新**：odps-sdk 已更新至 [0.48.6-public](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.50.x/CHANGELOG_CN.md#0486-public---2024-07-17)。
  - 新版本减少了网络开销，略微提高在执行离线作业时的效率。


## [3.5.7] - 2024-04-29

### 新增功能
- **新增参数 `tunnelDownloadUseSingleReader`**：默认值为 `false`。开启该参数后，每个 ResultSet 将仅使用单个 Reader 进行数据下载。
此改动适用于一次性读取大量数据的场景，以优化性能。原行为是在每次调用 ResultSet 的 `hasNext` 方法时开启一个 Reader，以防止连接中断。


## [3.5.6] - 2023-10-27
### 新增功能
- 支持 `insert into tablename (co1,co2) values(?,?)` 语法
- `DatabaseMetadata.getColumns` 适配使用三层模型的情况