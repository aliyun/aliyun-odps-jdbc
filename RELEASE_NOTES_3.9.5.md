# JDBC 3.9.5 — MaxQA 服务端 Fallback 兼容版本

本版本基于 **3.9.4**，面向暂时需要保留 3.9 系列 JDBC 行为及 `jar-with-dependencies` 打包方式的用户，回移 MaxQA 服务端 Fallback 支持，并修复结果集生命周期导致的下载线程泄漏。**Statement / ResultSet 生命周期导致的线程泄漏已修复；其余未回移修复与功能差异见下文。**

## 本次变更

- 修复 Statement 复用、未取出 ResultSet 就关闭 Statement、滚动结果集转换时遗留底层下载线程的问题（回移 3.10.5 资源释放修复）。
- 同时回移后续前向 ResultSet 所有权修复：包装时不提前关闭底层数据，关闭前向结果集时释放底层迭代器，避免重复关闭；滚动转换失败同样释放旧下载任务。滚动结果集初始化读取计数的临时 reader 通过 try-with-resources 释放。
- 将 `disableFallback` / `fallbackQuota` 接入 `MaxQAConnInfo.FallbackInfo`，由 SDK 通过 `x-odps-fallback-infos` 请求头提交给 MaxQA 服务端。
- 默认请求开启服务端 Fallback；`fallbackQuota` 可指定离线回退 quota，未指定时由服务端选择。`disableFallback=true` 不发送启用 Fallback 的配置，并优先于 `fallbackQuota`。
- 保留 3.9 的 `SET odps.task.wlm.quota` 动态切换：切换到新 MaxQA quota 时重新获取连接信息并保留 Fallback 配置，切回离线 quota 时清除旧 MaxQA 信息及执行模式。
- SDK 从 `0.52.2-public` 升级至 `0.61.2-public`，以获得所需服务端协议支持；补齐 `Record.clear()` 的空记录实现以兼容新版 SDK。依赖版本有变化，升级前仍需验证宿主应用兼容性。

连接 URL 示例（凭据通过 Properties 提供）：

```text
jdbc:odps:https://service.<region>.maxcompute.aliyun.com/api?project=<project>&interactiveMode=MaxQA&quotaName=<maxqa_quota>&fallbackQuota=<offline_quota>
```

关闭服务端 Fallback 请求：增加 `&disableFallback=true`。配置文件对应键为 `fallback_quota`、`disable_fallback`。使用者需具备目标 quota 权限；是否实际回退由服务端条件与配置决定。MCQA v1 的客户端 FallbackPolicy 与 MaxQA 的服务端 Fallback 是不同路径，不能将 v1 的逐类错误策略理解为 MaxQA 服务端策略。

## 已修复的问题

3.9.5 已修复 Statement 复用、未取出结果即关闭 Statement、前向结果集关闭，以及滚动结果集转换成功/失败时遗留底层下载任务的问题。前向结果集在关闭前仍可读取，底层迭代器只关闭一次。**这些生命周期线程泄漏不再属于本版本的保留问题。**

## 尚未回移的缺陷修复

| 未回移项 | 当前影响与使用限制 | 对应 3.10 修复 |
| --- | --- | --- |
| 下载迭代器自身的并发关闭协调 | 本次修复了调用方遗漏 `close()`；`InstanceDataIterator` 内部仍沿用 3.9 实现，未回移原子关闭状态、关闭期间停止提交任务及队列清理。并发读取与关闭同一结果集仍有竞态风险，避免跨线程同时操作同一结果集。 | [3.10.3](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.3) 的迭代器并发及关闭协调修复。 |
| Catalog / Schema 与 namespace 模式处理 | 三层模型下元数据查询及 catalog/schema 读写可能不符合预期，影响 BI 工具的库表浏览。 | [3.10.2](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.2) 修正相关查询、get/set 及配置键。 |
| 非 Tunnel INSERT 的影响行数获取使用固定 task 名 | task 名不匹配时，即使 SQL 成功也可能拿不到正确的 affected-row count；不要只凭该计数判断写入结果。 | [3.10.12](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.12) 根据实际 executor / instance task 名获取 TaskSummary。 |

上述项目是尚未回移的缺陷修复，不属于有意保留的功能差异。本版线程泄漏回归通过不代表这些问题也已解决。

## 保留的功能与打包差异

| 差异 | 3.9.5 行为与影响 |
| --- | --- |
| 标准 `java.sql.Array` 支持 | 未回移 3.10.0 新增的标准 ARRAY 接口支持。 |
| 依赖打包与隔离 | 保留 3.9 的 assembly / `jar-with-dependencies`，未采用 3.10.0 的完整 JDBC 依赖重定位及 3.10.1 的 Arrow/Netty 打包调整。宿主应用的 Netty、Arrow、Jackson 等仍可能冲突；SDK 自带的部分 shading 不等于 JDBC 全量隔离。 |
| 动态 quota 切换 | 保留 3.9 的 `SET odps.task.wlm.quota` 切换能力，并适配本次 Fallback 配置。 |

SDK 升级不代表获得了 3.10.13 对 JDBC 最终打包依赖的全部安全加固；**本版本不承诺消除全部依赖 CVE**，应按最终 JAR 与宿主 classpath 做安全评估。

受尚未回移的缺陷或上述功能限制影响的应用，应评估升级 3.10.13 或后续经过验证的版本。

## 制品与升级

- Maven 坐标：`com.aliyun.odps:odps-jdbc:3.9.5`。
- 独立驱动包：`odps-jdbc-3.9.5-jar-with-dependencies.jar`；普通 JAR 需要由 Maven 解析依赖。
- 替换旧 JDBC JAR，避免同时加载多个驱动版本；验证连接、查询、参数绑定、结果读取及业务侧 MaxQA 回退场景。
- 若兼容性验证失败，可恢复原 3.9.4 依赖/JAR；恢复后不具备本次新增的服务端 Fallback 配置能力，也会失去本次线程泄漏修复。
