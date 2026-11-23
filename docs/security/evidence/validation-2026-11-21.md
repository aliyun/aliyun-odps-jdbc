# 2026-11-21 评审轮复核读数（候选 head 待提交，base `d8aad4a04fcef8886750ac18de7bd67968c03f2f`）

本轮回应评审在 `2eabfe2764fda8945a98356579cd6c4b3e55ba7f` 上提的四条非阻塞意见，只做三类事：更正公告日期口径、补两条被数据源漏掉的公告面、把"兼容性覆盖"与"能红的回归"分开并补一条真正区分 CVE-2026-108106 的用例。环境：Linux x86_64，Maven 3.8.7，Ubuntu OpenJDK 8 与 21。

## 1. 构建与回归（与 11-11 同命令）

| 档位 | 命令 | 结果 |
| --- | --- | --- |
| JDK 8 | `JAVA_HOME=<jdk8> mvn -B -ntp clean package -Pemulator-ci -Dmaven.javadoc.skip=true` | BUILD SUCCESS，45.603 s，`Tests run: 186, Failures: 0, Errors: 0, Skipped: 0` |
| JDK 21 | 同上，`JAVA_HOME=<jdk21>` | BUILD SUCCESS，46.227 s，`Tests run: 186, Failures: 0, Errors: 0, Skipped: 0` |

用例数从 185 涨到 186，差值是新增的 `snappyRejectsDisproportionateDeclaredLengthBeforeAllocation`。

## 2. 新用例的成对读数（有牙齿的判据）

`JAVA_HOME=<jdk8> mvn -B -ntp test -Dtest=DependencyCompatibilityTest -Dsnappy.version=1.1.10.7`

```text
[ERROR] Tests run: 12, Failures: 2, Errors: 0, Skipped: 0 -- in com.aliyun.odps.jdbc.DependencyCompatibilityTest
[ERROR]   DependencyCompatibilityTest.snappyArrayCopyRejectsOutOfRangeArguments:213 Expected java.lang.IllegalArgumentException to be thrown, but nothing was thrown.
[ERROR]   DependencyCompatibilityTest.snappyRejectsDisproportionateDeclaredLengthBeforeAllocation:173 A declared length no stream of this size can expand to must be rejected before allocating ==> Expected java.io.IOException to be thrown, but nothing was thrown.
[INFO] BUILD FAILURE
```

同一套用例在 1.1.10.12 上 `Tests run: 12, Failures: 0`。独立探针（JDK 8，两个制品分别跑）给出机制形状：对只改写头部 varint、把声明长度抬到 `64/3 × 输入` 之外的帧，1.1.10.7 的 `Snappy.uncompressedLength()` 返回放大后的数字（声明 65,536 / 输入 25 字节，上限 533），负声明长度原样返回 `-1`；1.1.10.12 两处都抛 `org.xerial.snappy.SnappyIOException: [PARSING_ERROR] invalid uncompressed length ...`。上限之内（改写为 400）两版都返回 400，未改写原帧两版都返回真实长度 ⇒ 新用例断言的是那条 cap，不是"什么都不接受"。

按评审意见归类（不再混称）：`snappyRejectsTruncatedFramesBeforeNativeCopy` 的截断输入在两版都抛 `IOException`、`snappyTypedArrayRoundTripsMatchTunnelCodecUsage` 的合法往返在两版都成立，二者是**兼容性覆盖**，不作为 CVE-2026-108106 的回归证据；有牙齿的是上面这两条 Failures。

## 3. CI 制品的全量条目核对（加强后的工装）

`docs/security/evidence/verify_shaded_versions.py` 新增 `--inventory group:artifact=v1,v2 --prefix <path>`，比对某前缀下**全部非目录条目**（native 库、资源、service 文件），缺一条、多一条或字节不同都以非零码退出；重定位件不能逐字节比对的限制写进脚本头部。对 `packaged-driver` 制品（workflow run `44065424723`）：

| 读数 | 值 |
| --- | --- |
| JAR 大小 / SHA-256 | 56,115,919 字节 / `6fd483a8d63a4b27e8633acbc8ed5800ebd18334ac0d58a64252ab6f3a75ef55` |
| 条目数 / 重复条目名 / `ch/qos/logback` 条目 | 24,692 / 0 / 0 |
| `org/xerial/snappy/**` 非目录条目 | 70 |
| 对 Central 1.1.10.12 | compared 70，missing 0，extra 0，mismatch 0 ⇒ `exact_complete=true` |
| 对 Central 1.1.10.7（负对照） | compared 69，missing 0，**extra 1**，mismatch 45；多出的那条是 `org/xerial/snappy/native/Linux/x86_64-musl/libsnappyjava.so` |
| guava 自报版本 / 未受影响对照 netty-codec-http | 33.7.2-jre / 4.1.137.Final |

原来只看 40 个重叠 class 的 bytecheck 会漏掉"条目集合本身变了"这件事（musl native 库），这正是全量比对的收益。本机 `mvn` 产物的 SHA 与 CI 制品不同（本机 JDK21 档 56,115,484 字节 / `97a0e3c9aa06fa0e…`），差异在 MANIFEST 与时间戳一类条目，属仓内已知的构建可复现性问题，因此字节级结论一律锚在 CI 制品上。

## 4. 公告日期与数据源面（现查于 11-21）

| 公告 | OSV Maven 源命中 | 全局 GHSA 条目 published_at | 上游仓公告 published_at |
| --- | --- | --- | --- |
| GHSA-gwq9-9ffj-mvrv / CVE-2026-93451 | 是 | 2026-10-27 | 2026-10-03 |
| GHSA-q233-g92q-qxp6 / CVE-2026-108106 | 是 | 2026-10-27 | 2026-10-03 |
| GHSA-xqrx-qh46-34m7 / CVE-2026-116590 | 是 | 2026-10-27 | 2026-10-03 |
| GHSA-xxph-c9ww-hj94 / CVE-2026-102554 | 是 | 2026-10-27 | 2026-09-29 |
| GHSA-wmgv-28fv-894x / CVE-2026-90559 | **否** | 全局端点返回空 | 2026-10-03，范围 `<= 1.1.10.8` |
| GHSA-38f5-hf66-x965 | **否** | 全局端点返回空 | 2026-10-03，范围 `<= 1.1.10.8` |
| GHSA-6gp7-6wmv-gxqw | **否** | 全局端点返回空 | 2026-10-03，范围 `<= 1.1.10.9` |
| GHSA-c73m-r934-8qvg | **否** | 全局端点返回空 | 2026-10-03，范围 `<= 1.1.10.9` |
| GHSA-v5x9-67m3-7m56 / CVE-2026-118042（guava，low） | **否**（直查 33.7.2-jre → 0 命中） | — | 2026-10-31，范围 `>= 13.0, <= 33.7.2-jre` |

四条 snappy 公告虽未被本轮数据源返回，但 1.1.10.12 都落在它们的范围之外，所以修复面比按 OSV 计数得到的三条更宽。guava 那条相反：`33.7.2-jre` 自身在范围内，而 11-21 现查 Central 的 guava 最新号仍是 `33.7.2-jre` / `33.7.2-android`，没有可用的修复发布版可去；公告点名的 `HashBiMap` / `LinkedHashMultimap` 不在已检索的驱动与 SDK 使用面上，但本报告不据此称不可利用。

## 5. 本轮没有做的事

没有跑真实服务端的压缩链路（驱动不设置压缩算法，走 SDK 默认协商），没有跑发布/打 tag，没有对漏洞副本做崩溃性负向实验；对 SDK 自身 POM 的管理版本不作改动——直接使用 `odps-sdk-core` 的消费者仍需 SDK 侧另行修复与发布。

## 6. 11-24 对本文件第 4 节的更正

第 4 节表格里"guava 最新仍是 33.7.2-jre / 33.7.2-android，无修复版可去"这句是错的，错因是取数用的过滤只匹配 `33.6*` / `33.7*`。11-24 现查重取（`curl repo1.maven.org/maven2/com/google/guava/guava/maven-metadata.xml` ⇒ `<latest>33.8.0-jre</latest>`、`<lastUpdated>20261102180748`；`api.github.com/repos/google/guava/releases/tags/v33.8.0` ⇒ published_at 2026-11-02T18:08:43Z；同一公告 updated_at 2026-11-02T18:07:19Z）：**33.8.0-jre 自 2026-11-02 起就在 Central，且落在 `<= 33.7.2-jre` 之外**。

按 33.8.0-jre 跑的一次性对照（`mvn -B -ntp clean package -Pemulator-ci -Dmaven.javadoc.skip=true -Dguava.version=33.8.0-jre`）：

| 档位 | 结果 |
| --- | --- |
| JDK 8 | BUILD SUCCESS，`Tests run: 186, Failures: 0`，50.473 s |
| JDK 21 | BUILD SUCCESS，`Tests run: 186, Failures: 0`，46.463 s |

制品面：条目 24,692 → 24,702；重定位 guava 条目 1,982 → 1,990；`shaded/com/google/common/collect/HashBiMap$FloodedTables.class` 出现（该公告的修复类）；重定位的 `ImmutableList.class` / `CacheBuilder.class` 类文件主版本仍 52；`META-INF/versions/` 条目 43 → 45（新增 `versions/9/OSGI-INF/MANIFEST.MF`）。snappy 清单不受影响（70 条全等 1.1.10.12）。工装用法补一条：发行 JAR 里 `META-INF/maven/org.xerial.snappy/**` 命中 0，所以 snappy 没有自报版本可断言，`--expect` 对它必然失败——它的版本只能由 `--inventory` 的字节清单给出（我 11-24 一次调用误用就是这个形状）。

结论更正：这一格"可闭合"，但选号是否跨 minor 属维护者决定，本轮不把版本改动塞进他已 LGTM 的 head。
