# JDBC 3.9.5 — MaxQA 服务端 Fallback 兼容版本

本版本基于 **3.9.4**，面向暂时需要保留 3.9 系列 JDBC 行为及 `jar-with-dependencies` 打包方式的用户，回移 MaxQA 服务端 Fallback 支持。**本版本不是 3.10 的完整修复合集，下面列出的 3.9 已知问题仍然存在。**

## 本次变更

- 将 `disableFallback` / `fallbackQuota` 接入 `MaxQAConnInfo.FallbackInfo`，由 SDK 通过 `x-odps-fallback-infos` 请求头提交给 MaxQA 服务端。
- 默认请求开启服务端 Fallback；`fallbackQuota` 可指定离线回退 quota，未指定时由服务端选择。`disableFallback=true` 不发送启用 Fallback 的配置，并优先于 `fallbackQuota`。
- 保留 3.9 的 `SET odps.task.wlm.quota` 动态切换：切换到新 MaxQA quota 时重新获取连接信息并保留 Fallback 配置，切回离线 quota 时清除旧 MaxQA 信息及执行模式。
- SDK 从 `0.52.2-public` 升级至 `0.61.2-public`，以获得所需服务端协议支持；补齐 `Record.clear()` 的空记录实现以兼容新版 SDK。依赖版本有变化，升级前仍需验证宿主应用兼容性。

连接 URL 示例（凭据通过 Properties 提供）：

```text
jdbc:odps:https://service.<region>.maxcompute.aliyun.com/api?project=<project>&interactiveMode=MaxQA&quotaName=<maxqa_quota>&fallbackQuota=<offline_quota>
```

关闭服务端 Fallback 请求：增加 `&disableFallback=true`。配置文件对应键为 `fallback_quota`、`disable_fallback`。使用者需具备目标 quota 权限；是否实际回退由服务端条件与配置决定。MCQA v1 的客户端 FallbackPolicy 与 MaxQA 的服务端 Fallback 是不同路径，不能将 v1 的逐类错误策略理解为 MaxQA 服务端策略。

## 继续使用 3.9 的已知问题

| 保留问题 | 影响和临时措施 | 3.10 中的修复 |
| --- | --- | --- |
| Statement 复用 / 滚动 ResultSet 路径未完整关闭底层迭代器 | 下载线程可能持续累积，长期运行可能耗尽资源。显式关闭 ResultSet，尽量使用前向结果集且每次查询单独创建并关闭 Statement；这些措施不等于修复。 | [3.10.5](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.5) 修复释放引用前未关闭底层结果集的问题。 |
| 并发下载迭代器的关闭状态、任务队列和线程池释放不完善 | 并发消费或关闭结果集时存在竞态及资源释放风险；避免并发操作同一结果集。 | [3.10.3](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.3) 加入原子关闭状态及线程池、队列清理等修复。 |
| Catalog / Schema 与 namespace 模式处理的旧逻辑 | 三层模型下元数据查询及 catalog/schema 读写可能不符合预期，影响 BI 工具的库表浏览。 | [3.10.2](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.2) 修正相关查询、get/set 及配置键。 |
| 依赖隔离仍沿用 3.9 的 assembly 方式 | 未采用 3.10 的完整 JDBC 依赖重定位，宿主应用的 Netty、Arrow、Jackson 等仍可能冲突。SDK 自带的部分 shading 不等于 JDBC 全量隔离。 | [3.10.0](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.0) 引入依赖重定位，[3.10.1](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.1) 继续修正 Arrow/Netty 打包。 |
| 非 Tunnel INSERT 的影响行数获取仍使用固定 task 名 | 在 task 名不匹配时，即使 SQL 成功也可能拿不到正确的 affected-row count；不要只凭该计数判断写入结果。 | [3.10.12](https://github.com/aliyun/aliyun-odps-jdbc/releases/tag/v3.10.12) 根据实际 executor / instance task 名获取 TaskSummary。 |

此外，本版本未回移 3.10.0 的标准 `java.sql.Array` 支持。SDK 升级也不代表获得了 3.10.13 对 JDBC 最终打包依赖的全部安全加固；**本版本不承诺消除全部依赖 CVE**，应按最终 JAR 与宿主 classpath 做安全评估。

对于受以上问题影响的应用，建议评估升级 3.10.13 或后续经过验证的版本。本次选择 3.9.5 的目的仅是兼容接入最新 MaxQA Fallback。

## 制品与升级

- Maven 坐标：`com.aliyun.odps:odps-jdbc:3.9.5`。
- 独立驱动包：`odps-jdbc-3.9.5-jar-with-dependencies.jar`；普通 JAR 需要由 Maven 解析依赖。
- 替换旧 JDBC JAR，避免同时加载多个驱动版本；验证连接、查询、参数绑定、结果读取及业务侧 MaxQA 回退场景。
- 若兼容性验证失败，可恢复原 3.9.4 依赖/JAR；恢复后不具备本次新增的服务端 Fallback 配置能力。
