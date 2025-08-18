# JDBC 驱动时区处理：Pinned Timezone

> JDBC 参数中的 timezone 即代表 serverTimezone，代表数据库中存储的时间值，在读取时，会根据 serverTimezone 进行转换。

## 1. 背景与问题

在传统 JDBC 驱动的 **Point-in-Time 模式** 下，对于 `DATE`/`DATETIME`/`TIMESTAMP` 字段：

- 驱动会将值按 **JVM 默认时区**（或 `Calendar` 提供的时区）解析成 UTC 毫秒；
- 渲染到 Java `Timestamp`/`Date`/`Time` 时，又按 JVM 时区或指定 `Calendar` 反向转换。

在这种模式下，同一数据在不同 **JVM 默认时区** 或不同 **连接参数** 下，可能会出现**漂移效应**（时间部分变化）。

### 问题示例

假设 DB 存 `2020-01-01 00:00:00`，会话时区 = UTC：

| JVM 时区        | getTimestamp() 输出   |
|---------------|---------------------|
| UTC           | 2020-01-01 00:00:00 |
| Asia/Shanghai | 2020-01-01 08:00:00 |

这种现象在跨机房、跨环境的任务中，容易造成 **时间不一致** 和 **难以排查的 Bug**。

---

## 2. 设计目标

1. **一致性（Consistency）**
    - 读取与写入规则固定，不受 JVM 默认时区影响；
    - 同一数据在任何客户端、任何 JVM 时区下读取，结果一致。
2. **可控性（Configurability）**
    - 提供 `serverTimezone` 参数，允许用户/项目指定期望的“最终视图”时区。
    - 默认值来自 project-level 时区（获取当前 project 级别参数 `odps.sql.timezone`）。
3. **对称性（Symmetry）**
    - 读取：UTC → serverTimezone；
    - 写入：serverTimezone → UTC。

---

## 3. 核心设计

### 3.1 总则

- 受项目时区影响类型（`DATE` / `DATETIME` / `TIMESTAMP`）：
    - 读取：按 `serverTimezone` 解释 UTC 存储值，转换成目标时区的本地时间。
    - 写入：将 `serverTimezone` 下的本地时间转换成 UTC 存储。
- 不受项目时区类型（`TIMESTAMP_NTZ`）：
    - 不进行处理，原样展示。

### 3.2 serverTimezone 的定义与优先级

1. **获取规则**
    - 默认：从项目元数据 `serverTimezone` 读取作为默认值
    - 可通过连接参数 `serverTimezone` 显式配置，覆盖默认值；

2. **应用优先级**
    - 调用 `getXxx(col, Calendar)` → **优先使用 Calendar 时区** 渲染；
    - 未指定 Calendar → 使用 `serverTimezone`；
    - `serverTimezone` 对读取和写入均生效。

---

## 4. 行为规则说明

**MaxCompute 底层实际存储为 UTC 值**：`2012-01-01 00:00:00.000Z`

### 场景一：serverTimezone = Asia/Shanghai

| JVM 时区 | getDate / getTimestamp / getTime / getString 返回 |
|--------|-------------------------------------------------|
| 任何     | 2012-01-01 08:00:00                             |

### 场景二：serverTimezone = UTC

| JVM 时区 | getDate / getTimestamp / getTime / getString 返回 |
|--------|-------------------------------------------------|
| 任何     | 2012-01-01 00:00:00                             |

> 注：
> - `getString()` 在本模式下与 `getTimestamp()` 等一致，不再直接返回数据库原文。  
    >   驱动应在 `getString()` 内部应用同样的时区转换逻辑，保证四个读取接口一致。

---

## 5. 写入规则

> **保证读写对称性**，防止时间“往返飘移”

- `setTimestamp(col, ts)`：假定传入的 `ts` 为 `serverTimezone` 下的本地时间，先转成 UTC 再发给数据库。
- `setDate` / `setTime`：同上，补齐日期/时间部分后转 UTC 存储。
- 按 `Calendar` 重载传入时，以指定 Calendar 时区优先。

---

## 6. 与 JDBC 默认模式比较

| 项目                | JDBC 默认 Point-in-Time 模式 | 本设计 Pinned Timezone 模式 |
|-------------------|--------------------------|------------------------|
| JVM 默认时区影响        | 有                        | 无                      |
| serverTimezone 作用 | 仅部分驱动、部分类型支持             | 必须支持，且为全局核心            |
| getString 是否一致    | 可能与 getTimestamp 不一致     | 一致                     |
| 跨时区部署一致性          | 易飘移                      | 稳定                     |
| LocalDateTime 读取  | 原值                       | 原值                     |
| 写入对称性             | 依赖驱动实现                   | 强制对称                   |

---