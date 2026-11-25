# 2026-11 轮修复候选的构建与回归读数

执行日期：2026-11-11（Asia/Shanghai），Linux x86_64，Maven 3.8.7，JDK 8 / JDK 21（Ubuntu OpenJDK）。
所有命令在 `master` = `cf90deee55102253ccb49534c6e5ba1988a87d85` 的独立 worktree 上执行；候选改动只含 `pom.xml` 的两条 `dependencyManagement` 固定与 `DependencyCompatibilityTest` 的 4 个离线用例。

## 基线（未改动）

`JAVA_HOME=<jdk8> mvn -B -ntp clean package -Pemulator-ci -Dmaven.javadoc.skip=true`

- BUILD SUCCESS，Total time 45.777 s
- `Tests run: 173, Failures: 0, Errors: 0, Skipped: 0`
- 产物 `target/odps-jdbc-3.10.18.jar` 55,966,037 B（与 Central 发布的 55,965,968 B 相差 69 B，来自 MANIFEST/时间戳，不作为制品一致性的判据）

## 候选修复

| 档位 | 命令 | 结果 |
| --- | --- | --- |
| JDK 8 | `mvn -B -ntp clean package -Pemulator-ci -Dmaven.javadoc.skip=true` | BUILD SUCCESS，01:00 min，`Tests run: 177, Failures: 0, Errors: 0, Skipped: 0`（较基线 +4，即新增的 snappy/guava 用例） |
| JDK 21 | 同上，`JAVA_HOME=<jdk21>` | BUILD SUCCESS，46.475 s，`Tests run: 177, Failures: 0, Errors: 0, Skipped: 0` |

`com/aliyun/odps/jdbc/OdpsDriver.class` 类文件主版本仍为 52（Java 8），字节码目标未随构建 JDK 改变。

## 发行 JAR 内容核验

`python3 verify_shaded_versions.py --jar target/odps-jdbc-3.10.18.jar --expect com.google.guava:guava=33.7.2-jre --expect io.netty:netty-codec-http=4.1.137.Final --expect com.aliyun.odps:odps-jdbc=3.10.18 --bytecheck org.xerial.snappy:snappy-java=1.1.10.7,1.1.10.12`

| 读数 | Central 3.10.18 发布件 | 候选构建产物 |
| --- | --- | --- |
| JAR 内 guava `pom.properties` 自报 | 33.4.8-jre | 33.7.2-jre |
| JAR 内 netty-codec-http 自报 | 4.1.137.Final | 4.1.137.Final（未受影响项作对照） |
| snappy 40 个未重定位类的字节比对 | 1.1.10.7 全匹配（mismatch 0）；1.1.10.12 全部不匹配（mismatch 40） | 1.1.10.12 全匹配（mismatch 0）；1.1.10.7 全部不匹配（mismatch 40） |
| 条目数 / 重复条目名 | 24,685 / 0 | 24,691 / 0 |
| 字节数 | 55965968 | 56115051 |
| SHA-256（候选产物） | — | `5a88b00407ded13466ebb013ef746f627a678d60e561b68fb930876aaf133883` |

体积变化归因：snappy-java 自身制品 2,338,496 → 2,448,671 B（+110,175，含 native 库），guava 自身制品 3,023,797 → 3,057,659 B（+33,862）；uber JAR 实测 +149,014 B，差额来自各条目重新压缩与条目数变化（`org/xerial/snappy/**` 106 → 108，重定位后的 `com/google/common/**` 1,986 → 1,982）。只作量级归因，不作逐字节等式。

## 行为级回归（用例有牙齿的判据）

`JAVA_HOME=<jdk8> mvn -B -ntp test -Dtest=DependencyCompatibilityTest -Dsnappy.version=1.1.10.7`

```text
[ERROR] Tests run: 11, Failures: 1, Errors: 0, Skipped: 0 -- in com.aliyun.odps.jdbc.DependencyCompatibilityTest
[ERROR]   DependencyCompatibilityTest.snappyArrayCopyRejectsOutOfRangeArguments:157 Expected java.lang.IllegalArgumentException to be thrown, but nothing was thrown.
[INFO] BUILD FAILURE
```

同一用例在 1.1.10.12 上通过。独立探针（JDK 8，直接对两个 snappy 制品运行同一组调用）复核：`Snappy.arrayCopy(payload, 0, len, new byte[1], 0)` 与负偏移调用在 1.1.10.7 上不抛异常（越界拷贝静默执行，即 CVE-2026-116590 缺的界检查），在 1.1.10.12 上抛 `IllegalArgumentException`；`uncompressedLength` / `uncompress` 对截断帧在两版均抛 `IOException`；`char[]` / `int[]` / `long[]` typed-array 往返在两版均相等（兼容性未被固定项破坏）。

## 复扫

- 候选解析树：`python3 scan_maven_trees.py` → 100 个坐标，命中 1 个包，即 `ch.qos.logback:logback-core` 1.3.16（scope=test，3 条公告）。
- 候选发行 JAR 内 65 个非自有坐标：命中 0。
- 未覆盖范围与前提见 `../dependency-cve-assessment-2026-11-11.md`。真实服务 SQL 冒烟与发布不在本轮读数内。
